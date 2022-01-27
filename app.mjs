import fs from 'fs'
import path from 'path'
import { pointsToH3 } from './custom_modules/pointToH3.mjs'
import { polygonsToH3 } from './custom_modules/polygonsToH3.mjs'
import StreamArray from 'stream-json/streamers/StreamArray.js'
import Pick from 'stream-json/filters/Pick.js'
import batch from 'stream-json/utils/Batch.js'
import chain from 'stream-chain'

/* DESCRIPTION
   Script accepts 2 arguments:
   1) geoJSON file name (required)
   2) DGGS H3 resolution (optional)
*/

/* COMMAND LINE EXAMPLE
   "node app.mjs modis.geojson 5"
*/

// run app
app()

// PURE FUNCTIONS
// ====================================================================

async function app() {
  // conlsole arguments
  const args = process.argv.slice(2)
  const inComeFilename = args[0]
  const inComeResolution = consoleArgCheck(args[1])

  const pathBaseName = path.basename(inComeFilename, '.geojson')
  const tableName = pathBaseName.replace(/-/g, '_').replace(/ /g, '_')

  // files URLs relative
  const filesToConvertFolder = './files/geoJsonToConvert/'
  const databaseUrl = './db/database.db'
  const convertedJSONFolder = './files/resultGeojson/' // default: ./files/resultGeojson/

  // quantity of json objects per 1 chunk
  const meanBatchSizeDefine = 100000
  const meanBatchSizeInsert = 10000

  // arguments of conversion function (points, polygons)
  const params = [
    inComeFilename,
    inComeResolution,
    meanBatchSizeDefine,
    meanBatchSizeInsert,
    filesToConvertFolder,
    tableName,
    databaseUrl,
    convertedJSONFolder,
  ]

  // define geometry type
  const pipelineDefine = new chain([
    fs.createReadStream(filesToConvertFolder + inComeFilename),
    Pick.withParser({ filter: 'features' }),
    new StreamArray(),
    new batch({ batchSize: 1 }),
  ])
  await pipelineDefine.on('data', (features) => {
    const geometryType = features[0].value.geometry.type
    pipelineDefine.pause()
    switch (geometryType) {
      case 'Point':
        pointsToH3(...params)
        break
      case 'Polygon':
      case 'MultiPolygon':
        polygonsToH3(...params)
        break
      default:
        console.log('Unknown geometry')
    }
  })
}

// console argument check
function consoleArgCheck(arg) {
  if (arg !== undefined) {
    return arg
  } else {
    return null
  }
}
