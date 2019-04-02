import fs from "fs";
import pathlib from "path";
import urllib from "url";

import sqlite3, { Database } from "better-sqlite3";
import leveldown, { Bytes, LevelDown, LevelDownIterator } from "leveldown";
import levelup, { LevelUp } from "levelup";

import { locateChromeRoot } from "./locate";
import { pathExists } from "./util";

const hostKeySeparator = "\u0000\u0001";

/**
 * NOTE: this closes the db automatically. This is a weird idiom since
 * we're being given the db, but it simplifies the code
 */
async function *readAllFromDb(db: Database) {
    try {
        const rows = db.prepare(`
            SELECT key, value
            FROM ItemTable
        `).iterate();

        for (const row of rows) {
            yield row;
        }
    } finally {
        db.close();
    }
}

function keyFromUrl(url: string, key?: string, separator: string = hostKeySeparator) {
    const suffix = key
        ? `${separator}${key}`
        : "";

    const parsed = urllib.parse(url);
    const { host } = parsed;
    let { protocol } = parsed;
    if (!host) throw new Error("host is required");
    if (!protocol) protocol = "https:";

    return Buffer.from(`_${protocol}//${host}${suffix}`);
}

function unpackValue(v: Bytes) {
    // values are apparently prefixed with `\u0001`
    return v.toString().slice(1);
}

async function *asyncIterable(
    iterator: LevelDownIterator,
) {
    try {
        while (true) {
            const next = await new Promise<{
                key: Bytes,
                value: Bytes,
            } | undefined>((resolve, reject) => {
                iterator.next((err, key, value) => {
                    if (err) return reject(err);
                    if (key === undefined && value === undefined) {
                        resolve();
                        return;
                    }
                    resolve({key, value});
                });
            });

            // undefined means no more values
            if (!next) return;

            // yield the result
            yield next;
        }
    } finally {
        await new Promise((resolve, reject) => {
            iterator.end(err => {
                if (err) reject(err);
                else resolve();
            });
        });
    }
}

/**
 * The default levelup factory doesn't actually play nicely with
 * promises at all, throwing an uncatchable error outside of any
 * promises returned by other methods. So, let's make out own,
 * because that's stupid.
 */
function createAsyncLevelUpFactory(dbPath: string) {
    return () => new Promise<LevelUp<LevelDown>>((resolve, reject) => {
        const db = levelup(leveldown(dbPath), (err: Error) => {
            if (err) reject (err);
            else resolve(db);
        });
    });
}

export class LocalStorageExtractor {
    public static async create() {
        const root = await locateChromeRoot();
        const dbPath = pathlib.join(root, "Local Storage", "leveldb");
        return new LocalStorageExtractor(
            dbPath,
            createAsyncLevelUpFactory(dbPath),
        );
    }

    /** @internal */
    constructor(
        private dbPath: string,
        private openDb: () => Promise<LevelUp<LevelDown>>,
    ) {}

    public async read(url: string, key: string) {
        const db = await this.openDb();
        try {
            const v = await db.get(keyFromUrl(url, key));
            return unpackValue(v);
        } finally {
            db.close();
        }
    }

    public async *readAll(url: string) {
        // NOTE: the DB uses the sequence `\u0000\u0001` to separate
        // the host from the key, so we scan through to `\u0001` to
        // get all key-value pairs from a host
        const start = keyFromUrl(url);
        const end = keyFromUrl(url, "\u0001", "");

        const db = await this.openDb();

        try {

            // the @types don't include this for some reason...
            const stream = (db as any).iterator({
                gte: start,
                lte: end,
            }) as LevelDownIterator;

            for await (const entry of asyncIterable(stream)) {
                const { key, value } = entry as any as { key: Bytes, value: Bytes };

                const [ , actualKey ] = key.toString().split(hostKeySeparator);

                yield {
                    key: actualKey,
                    value: unpackValue(value),
                };
            }
        } finally {
            db.close();
        }

    }

}
