import fs from "fs";
import os from "os";
import path from "path";
import util from "util";

const pathExists = util.promisify(fs.exists);

const paths: { [p: string]: string[] } = {
    win32: [
        "C:\Users\%USERNAME%\AppData\Local\Google\Chrome\User Data\Default",
        "C:\Users\%USERNAME%\AppData\Local\Chromium\User Data\Default",
    ],

    darwin: [
        "~/Library/Application Support/Google/Chrome/Default",
        "~/Library/Application Support/Chromium/Default",
    ],

    linux: [
        "~/.config/google-chrome/Default",
        "~/.config/chromium/Default",
    ],
};

export async function locateChromeRoot() {
    const platform = paths[os.platform()];
    if (!platform) {
        throw new Error("Unsupported platform");
    }

    const username = os.userInfo().username;
    const home = os.homedir();

    const results = await Promise.all(platform.map(async candidate => {
        const replaced = candidate.replace("%USERNAME%", username)
            .replace(/^~/, home);
        const p = path.resolve(replaced);
        const exists = await pathExists(p);
        if (exists) return p;
    }));

    const chosen = results.find(it => it !== undefined);
    if (!chosen) throw new Error(`Couldn't find chrome root for platform ${platform}`);

    return chosen;
}
