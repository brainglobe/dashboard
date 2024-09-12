import { asyncBufferFromUrl, asyncBufferFromFile, parquetRead } from 'hyparquet'
import parquetjs from '@dsnp/parquetjs'

const url = 'https://anaconda-package-data.s3.amazonaws.com/conda/monthly/2023/2023-01.parquet'
const fileName = '/home/igor/2024-01.parquet'

// await parquetRead({
//     file: await asyncBufferFromUrl(url),
//     // rowStart: 0,
//     // rowEnd: 30,
//     onComplete: (data) => {
//       data.filter((row) => row[1].includes("torch")).forEach((row) => {
//         console.log(row)
//       })
//     }
//   }
// )

// await parquetRead({
//     file: await asyncBufferFromFile(fileName),
//     rowFormat: 'object',
//     rowStart: 800000,
//     rowEnd: 800010,
//     // rowEnd: 30,
//     onComplete: data => console.log(data)
//   }
// )

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

let reader = await parquetjs.ParquetReader.openFile(fileName)
let cursor = reader.getCursor(['pkg_name', 'counts'])

let record = null
let total_count = 0n

while (record = await cursor.next()) {
    if (brainglobe_packages.includes(record.pkg_name)) {
        console.log(record)
        total_count += record.counts
    }
}

console.log(total_count)

await reader.close()
