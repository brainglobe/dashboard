import parquetjs from '@dsnp/parquetjs';
import fs from 'fs';
import os from 'os';
import path, { resolve } from 'path';
import * as https from 'node:https';

interface CondaRecord {
    time: string,
    data_source: string,
    pkg_name: string,
    pkg_version: string,
    pkg_platform: string,
    pkg_python: string,
    counts: bigint,
}

async function downloadParquetFile(url: string, outputPath: string) {
    return new Promise((resolve, reject) => {
        const file = fs.createWriteStream(outputPath);
        https.get(url, (response) => {
            if (response.statusCode !== 200) {
                reject(new Error(`Failed to get '${url}' (${response.statusCode})`));
                return;
            }
            response.pipe(file);
            file.on('finish', () => {
                file.close(resolve);
            });
        }).on('error', (err) => {
            fs.unlink(outputPath, () => reject(err));
        });
    });
}

export const getCondaData = async (brainglobePackages: string[]) => {
    const baseDir = path.join(os.homedir(), '.dashboard');
    const legacyPackages = JSON.parse(fs.readFileSync(path.resolve('../brainglobe_legacy.json'), 'utf-8'));
    const legacyPackagesMap = new Map<string, string>(Object.entries(legacyPackages));

    console.log(legacyPackagesMap.get('bg-space'));
    if (!fs.existsSync(baseDir)) {
        fs.mkdirSync(baseDir);
    }

    const extractedRows: CondaRecord[] = [];
    for (let i = 2019; i <= 2024; i++) {
        for (let j = 1; j <= 12; j++) {
            const fileName = path.join(baseDir, `${i}-${String(j).padStart(2, '0')}.parquet`);

            if (!fs.existsSync(fileName)) {
                const url = `https://anaconda-package-data.s3.amazonaws.com/conda/monthly/${i}/${i}-${String(j).padStart(2, '0')}.parquet`;

                const checkURLReq = await new Promise((resolve, reject) => {
                    fetch(url, {
                        method: "HEAD"
                    }).then(response => {
                        resolve(response.status.toString()[0] === "2")
                    }).catch(error => {
                        reject(false)
                    })
                })

                if (!checkURLReq) {
                    break;
                } else {
                    await downloadParquetFile(url, fileName);
                }
            }

            let reader = await parquetjs.ParquetReader.openFile(fileName);
            let cursor = reader.getCursor();

            let record = null;
            while (record = await cursor.next() as CondaRecord) {
                if (brainglobePackages.includes(record.pkg_name)) {
                    extractedRows.push(record);
                }
            }

            await reader.close();
        }
    }

    const packageDownloads = new Map<string, bigint>();
    extractedRows.forEach((record) => {
        if (packageDownloads.has(record.pkg_name)) {
            packageDownloads.set(record.pkg_name, packageDownloads.get(record.pkg_name)! + record.counts);
        } else {
            packageDownloads.set(record.pkg_name, record.counts);
        }
    });
};

const brainglobe_packages = [
    "brainreg",
    "brainreg-napari",
    "brainreg-segment",
    "cellfinder-core",
    "cellfinder-napari",
    "brainglobe-space",
    "brainglobe-utils",
    "brainglobe-atlasapi",
    "brainglobe-napari-io",
    "brainglobe-segmentation",
]

getCondaData(brainglobe_packages)
