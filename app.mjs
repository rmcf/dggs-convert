import fs from 'fs'
import path from 'path'
import { pointsToH3 } from './custom_modules/pointToH3.mjs'
import StreamArray from 'stream-json/streamers/StreamArray.js'
import Pick from 'stream-json/filters/Pick.js'
import batch from 'stream-json/utils/Batch.js'
import chain from 'stream-chain'

// run app
mainApp()

// PURE FUNCTIONS
// ====================================================================

async function mainApp() {
  // conlsole arguments
  const args = process.argv.slice(2)
  const inComeFilename = args[0]
  const inComeResolution = consoleArgCheck(args[1])

  const pathBaseName = path.basename(inComeFilename, '.geojson')
  const tableName = pathBaseName.replace(/-/g, '_').replace(/ /g, '_')

  // files URLs relative
  const filesToConvertFolder = './files/geoJsonToConvert/'
  const databaseUrl = './db/database.db'

  // quantity of json objects per 1 chunk
  const meanBatchSizeDefine = 100000
  const meanBatchSizeInsert = 10000

  // define geometry type
  const pipelineDefine = new chain([
    fs.createReadStream(filesToConvertFolder + inComeFilename),
    Pick.withParser({ filter: 'features' }),
    new StreamArray(),
    new batch({ batchSize: 1 }),
  ])
  await pipelineDefine.on('data', (features) => {
    var geometryType = features[0].value.geometry.type
    pipelineDefine.pause()
    console.log('Geometry type ' + geometryType)
    if (geometryType === 'Point') {
      pointsToH3(
        inComeFilename,
        inComeResolution,
        meanBatchSizeDefine,
        meanBatchSizeInsert,
        filesToConvertFolder,
        tableName,
        databaseUrl
      )
    } else {
      console.log('Not point')
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
