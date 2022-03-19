import { execFile } from "child_process";
import crypto from "crypto";
import os from "os";
import pathlib from "path";
import urllib from "url";
import util from "util";

import connect, { Database } from "better-sqlite3";
import keytar from "keytar";
import { getDomain } from "tldjs";
import { domainMatch, pathMatch } from "tough-cookie";

import { locateChromeRoot } from "./locate";

const pbkdf2 = util.promisify(crypto.pbkdf2);

const KEYLENGTH = 16;
const SALT = "saltysalt";
const ITERATIONS = os.platform() === "darwin"
    ? 1003
    : 1;

/**
 * Util for extracting cookies from Chrome for one
 * or more URLs
 */
export class CookieExtractor {
    public static async create() {
        const root = await locateChromeRoot();
        return new CookieExtractor(
            connect(pathlib.join(root, "Cookies")),
        );
    }

    private cachedKey: Buffer | undefined;

    /** @internal */
    constructor(private db: Database) { }

    public close() {
        this.db.close();
    }

    public async loadMap(url: string) {
        const result: any = {};

        for await (const cookie of this.query(url)) {
            result[cookie.name] = cookie.value;
        }

        return result;
    }

    /**
     * Generate a sequence of cookie objects. On macOS
     * this may generate a prompt asking for permissions
     */
    public async *query(url: string) {
        const domain = getDomain(url);
        const { host, path, protocol } = urllib.parse(url);
        const requestedSecure = protocol === "https:";

        // ORDER BY tries to match sort order specified in
        // RFC 6265 - Section 5.4, step 2
        // http://tools.ietf.org/html/rfc6265#section-5.4
        // (from chrome-cookies-secure)
        const rows = this.db.prepare(`
            SELECT *
            FROM cookies
            WHERE host_key LIKE ?
            ORDER BY LENGTH(path) DESC, creation_utc ASC
        `).iterate(`%${domain}`);

        const values: any = {};
        const order: string[] = [];

        // First, pass through each result, adding to `values` and
        // `order`. This ensures that we only return the most recently
        // created versions of each cookie name
        for (const row of rows) {
            if (
                (row.is_secure && !requestedSecure)
                || (host && !domainMatch(host, row.host_key, true))
                || (path && !pathMatch(path, row.path))
            ) {
                // filter out non-matching cookie
                continue;
            }

            const old = values[row.name];
            values[row.name] = row;

            if (!old) {
                // new cookie? record its order
                order.push(row.name);
            }
        }

        let key: Buffer | undefined;

        // yield unique cookies in order, decrypting values as needed
        for (const name of order) {
            const cookie = values[name];
            if (cookie.value === "" && cookie.encrypted_value.length > 0) {

                if (!key) {
                    // lazy key fetch, on the off chance that
                    // we didn't actually need to decrypt anything
                    key = await this.getKey();
                }

                const encryptedValue = cookie.encrypted_value;
                cookie.value = decrypt(key, encryptedValue);
                delete cookie.encrypted_value;
            }

            yield cookie;
        }
    }

    private async getKey() {
        if (this.cachedKey) return this.cachedKey;

        let chromePassword = "peanuts";
        if (os.platform() === "darwin") {
            const fetched = await this.getPassword("Chrome Safe Storage", "Chrome");
            if (!fetched) {
                throw new Error("Not granted access to cookies");
            }

            chromePassword = fetched;
        }

        const decrypted = await pbkdf2(
            chromePassword, SALT, ITERATIONS, KEYLENGTH, "sha1",
        );

        this.cachedKey = decrypted;
        return decrypted;
    }

    private async getPassword(service: string, account: string) {
        try {
            return await keytar.getPassword(service, account);
        } catch (e) {
            if (
                os.platform() === "darwin"
                && e instanceof Error
                && e.message
                && e.message === "The user name or passphrase you entered is not correct."
            ) {
                // this can happen on macos sometimes for some reason...
                return this.findPasswordMacosFallback(service, account);
            }

            throw e;
        }
    }

    private async findPasswordMacosFallback(
        service: string,
        account: string,
    ) {
        return new Promise<string>((resolve, reject) => {
            execFile("security", [
                "find-generic-password",
                "-s", service,
                "-a", account,
                "-g",
            ], (err, _, stderr) => {
                if (err) reject(err);

                const str = stderr.toString();
                const m = str.match(/password: "([^"]+)"/);

                if (m) resolve(m[1]);
                else reject(new Error("Couldn't find password"));
            });
        });
    }
}

// Decryption based on chrome-secure-cookies,
// in turn based on:
// http://n8henrie.com/2014/05/decrypt-chrome-cookies-with-python/

function decrypt(key: Buffer, encryptedData: Buffer) {
    const iv = Buffer.from(new Array(KEYLENGTH + 1).join(" "), "binary");

    const decipher = crypto.createDecipheriv("aes-128-cbc", key, iv);
    decipher.setAutoPadding(false);

    encryptedData = encryptedData.slice(3);

    let decoded = decipher.update(encryptedData);

    const final = decipher.final();
    final.copy(decoded, decoded.length - 1);

    const padding = decoded[decoded.length - 1];
    if (padding) {
        decoded = decoded.slice(0, decoded.length - padding);
    }

    return decoded.toString("utf8");
}
