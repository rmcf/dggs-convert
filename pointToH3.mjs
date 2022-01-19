import fs from 'fs'
import path from 'path'
import StreamArray from 'stream-json/streamers/StreamArray.js'
import Pick from 'stream-json/filters/Pick.js'
import batch from 'stream-json/utils/Batch.js'
import chain from 'stream-chain'
import sqlite3 from 'sqlite3'
import { open } from 'sqlite'
import { geoToH3 } from 'h3-js'
import maxBy from 'lodash.maxby'
import minBy from 'lodash.minby'
import filter from 'lodash.filter'
sqlite3.verbose()

// conlsole arguments
const args = process.argv.slice(2)
const inComeFilename = args[0]
const tableName = path.basename(args[0], '.geojson')
const inComeResolution = consoleArgCheck(args[1])

// files URLs relative
const fileConvertUrl = './files/geoJsonToConvert/'
const databaseUrl = './db/db7.db'

// quantity of json objects per 1 chunk
const meanBatchSizeDefine = 100000
const meanBatchSizeInsert = 10000

// read geoJSON as stream
getJsonStream(
  inComeFilename,
  inComeResolution,
  meanBatchSizeDefine,
  meanBatchSizeInsert
)

// PURE FUNCTIONS
// ====================================================================

// get features form geoJSON as stream
async function getJsonStream(
  fileName,
  resolution,
  batchSizeDefine,
  batchSizeInsert
) {
  console.log('Begin stream')
  console.log('...')

  var namesTypesKeyValue = null // DB table fields names and types as key-value
  var optResolution = 0

  // Loop to DEFINE table types and names
  const pipelineDefine = new chain([
    fs.createReadStream(fileConvertUrl + fileName),
    Pick.withParser({ filter: 'features' }),
    new StreamArray(),
    new batch({ batchSize: batchSizeDefine }),
  ])
  await pipelineDefine.on('data', (features) => {
    checkPointsTopologyStream(features)
    const hexagonsAndResolution = conversionStream(features, resolution)
    const pipeHexagons = hexagonsAndResolution.hexagons
    const pipeResolution = hexagonsAndResolution.optResolution
    // optimal resolution - highest value from all chunks
    if (optResolution < pipeResolution) {
      optResolution = pipeResolution
    }
    // field types in the first chunk
    if (!namesTypesKeyValue) {
      namesTypesKeyValue = defineDBfieldTypes(pipeHexagons)
      console.log(namesTypesKeyValue)
    } else {
      // for null values
      checkValuesForNotNull(namesTypesKeyValue, pipeHexagons)
      // for integer values
      checkIntegerForReal(namesTypesKeyValue, pipeHexagons)
    }
  })
  await pipelineDefine.on('end', async () => {
    // replace null values with text
    namesTypesKeyValue.forEach((el) => {
      if (el.type === null) {
        el.type = 'TEXT'
      }
    })
    let names = namesTypesKeyValue.map((item) => item.name)
    console.log(names)
    let namesPlusTypes = namesTypesKeyValue.map((item) => {
      return item.name + ' ' + item.type
    })
    console.log(namesPlusTypes)

    // names to string
    const tableFieldsNamesString = names.join(',')
    // names and types to string
    const tableFieldsTypesString = namesPlusTypes.join(',')

    const db = await open({
      filename: databaseUrl,
      driver: sqlite3.Database,
    })
    await db.run(`DROP TABLE IF EXISTS ${tableName}`)
    await db.run(`CREATE TABLE ${tableName}(${tableFieldsTypesString})`)
    await db.close()
    console.log('Define completed with optReslution: ' + optResolution)

    // Loop to INSERT data into DB
    const dbins = await open({
      filename: databaseUrl,
      driver: sqlite3.Database,
    })
    const pipeline = new chain([
      fs.createReadStream(fileConvertUrl + fileName),
      Pick.withParser({ filter: 'features' }),
      new StreamArray(),
      new batch({ batchSize: batchSizeInsert }),
      (data) => {
        var features = data
        const hexagons = conversionStream(features, optResolution).hexagons
        var valuesSql = []
        for (const hex of hexagons) {
          let hexValues = []
          Object.entries(hex).forEach(([key, value]) => {
            let valueText = null
            if (value === null) {
              valueText = 'null'
            } else {
              if (typeof value === 'string') {
                valueText = '"' + value + '"'
              } else valueText = value
            }
            hexValues.push(valueText)
          })
          var hexValuesString = hexValues.join(', ')
          var valuesAndSqlSingleHex = `INSERT INTO ${tableName} (${tableFieldsNamesString}) VALUES (${hexValuesString})`
          valuesSql.push(valuesAndSqlSingleHex)
        }
        return valuesSql
      },
      new batch({ batchSize: batchSizeInsert }),
    ])
    await pipeline.on('data', (data) => {
      data.forEach(async (query, i) => {
        try {
          let d = new Date()
          let t = d.getTime()
          await dbins.run(query, [])
          console.log('Insert finished ' + i)
        } catch (error) {
          console.log(error)
        }
      })
    })
    await pipeline.on('end', async () => {
      try {
        await dbins.close()
      } catch (error) {
        console.log(error)
      }
      console.log('Database is closed')
    })
  })
}

// formatting numbers to string: 2 => "02"
function numberFormat(num) {
  if (num < 10) {
    return '0' + num
  } else return '' + num
}

// log results of transformation
function transformLog(level, allHex, uniqHex) {
  console.log('level: ' + level + ' | all: ' + allHex + ' | uniq: ' + uniqHex)
}

// convert geoJSON features to H3 array
function geoJsonFeaturesToH3Stream(features, resolution, attributes) {
  let h3IndexesArray = []
  let hexagons = features.map((feature) => {
    let long = feature.value.geometry.coordinates[0]
    let lat = feature.value.geometry.coordinates[1]
    let h3Index = geoToH3(lat, long, resolution)
    h3IndexesArray.push(h3Index)
    let hexagon = { H3INDEX: h3Index }
    if (attributes === 'attributes') {
      Object.entries(feature.value.properties).forEach(([key, value]) => {
        hexagon[key] = value
      })
    }
    return hexagon
  })
  let h3IndexesArrayUnique = Array.from(new Set(h3IndexesArray))
  // logging result
  transformLog(
    numberFormat(resolution),
    h3IndexesArray.length,
    h3IndexesArrayUnique.length
  )
  return {
    hex: hexagons,
    hexAllQuantity: h3IndexesArray.length,
    hexUniqQuantity: h3IndexesArrayUnique.length,
  }
}

// selection lowest resolution among highest hexs quantity
function optimalResolution(array) {
  const maxUniqHexQuantity = maxBy(array, 'uniqhexs')
  const maxUiqHexs = filter(array, ['uniqhexs', maxUniqHexQuantity.uniqhexs])
  const minResolutionLevel = minBy(maxUiqHexs, 'level')
  return minResolutionLevel.level
}

// check points topology stream
function checkPointsTopologyStream(features) {
  const coordinatesString = features.map((feature) => {
    let long = feature.value.geometry.coordinates[0]
    let lat = feature.value.geometry.coordinates[1]
    let coord = '' + long + lat
    return coord
  })
  const coordinatesStringUniq = Array.from(new Set(coordinatesString))
  if (coordinatesString.length !== coordinatesStringUniq.length) {
    let differences = coordinatesString.length - coordinatesStringUniq.length
    console.log('-------------------------------------')
    console.log(
      'There are ' +
        differences +
        ' points with the same coordiantes in dataset'
    )
    console.log('-------------------------------------')
  }
}

// convert features to hexagons H3
function conversionStream(features, res) {
  if (res) {
    let resolution = res
    let hexagons = geoJsonFeaturesToH3Stream(
      features,
      resolution,
      'attributes'
    ).hex
    return {
      hexagons: hexagons,
      optResolution: res,
    }
  } else {
    var startResolution = 1
    var conversionResult = {} // result of conversion function
    var uniqHexAtLevel = [] // hexagon quantity at each resolution level
    do {
      conversionResult = geoJsonFeaturesToH3Stream(features, startResolution)
      uniqHexAtLevel.push({
        level: startResolution,
        uniqhexs: conversionResult.hexUniqQuantity,
      })
      startResolution++
    } while (
      !(conversionResult.hexAllQuantity === conversionResult.hexUniqQuantity) &&
      startResolution <= 15
    )
    const optResolution = optimalResolution(uniqHexAtLevel)
    console.log('Optimal resolution for chunk: ' + optResolution)
    let hexagons = geoJsonFeaturesToH3Stream(
      features,
      optResolution,
      'attributes'
    ).hex
    return {
      hexagons: hexagons,
      optResolution: optResolution,
    }
  }
}

// define fields names and types for sqlite table
function defineDBfieldTypes(hexagons) {
  var tableTypes = []
  Object.entries(hexagons[0]).forEach(([key, value]) => {
    var type = null
    let hexagon = hexagons.find((hex) => hex[key] !== null)
    if (hexagon === undefined) {
      type = { name: key, type: null }
    } else {
      type = { name: key, type: typeDefine(hexagon[key]) }
    }
    tableTypes.push(type)
  })
  // for integer values
  checkIntegerForReal(tableTypes, hexagons)
  return tableTypes
}

// type define
function typeDefine(value) {
  const fieldTypeOf = typeof value
  var type = null
  switch (fieldTypeOf) {
    case 'string':
      type = 'TEXT'
      break
    case 'number':
      type = checkInteger(value)
      break
    case 'boolean':
      type = 'TEXT'
      break
    case 'object':
      console.log('|||||||||||||||||||||||||||||||> Object: ' + value)
      break
    default:
      type = null
      break
  }
  return type
}

// check number is it integer
function checkInteger(value) {
  if (Number.isInteger(value)) {
    return 'INTEGER'
  } else {
    return 'REAL'
  }
}

// check array with integer for real values
function checkIntegerForReal(arrayNameType, hexagons) {
  arrayNameType.forEach((el) => {
    if (el.type === 'INTEGER') {
      hexagons.forEach((hex) => {
        if (!Number.isInteger(hex[el.name])) {
          el.type = 'REAL'
        }
      })
    }
  })
}

// check array with null values for not null
function checkValuesForNotNull(arrayNameType, hexagons) {
  arrayNameType.forEach((el) => {
    if (el.type === null) {
      let hexagon = hexagons.find((hex) => hex[el.name] !== null)
      if (hexagon !== undefined) {
        el.type = typeDefine(hexagon[el.name])
      }
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
