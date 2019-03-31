import fs from "fs";
import util from "util";

export const pathExists = util.promisify(fs.exists);
