import fs from 'fs'
import StreamArray from 'stream-json/streamers/StreamArray.js'
import Pick from 'stream-json/filters/Pick.js'
import batch from 'stream-json/utils/Batch.js'
import chain from 'stream-chain'
import sqlite3 from 'sqlite3'
import { open } from 'sqlite'
import { polyfill } from 'h3-js'
import maxBy from 'lodash.maxby'
import minBy from 'lodash.minby'
import filter from 'lodash.filter'
import { h3toGeoJsonFile } from './h3ToGeoJSONfile.mjs'
sqlite3.verbose()

// get features form geoJSON as stream
export async function polygonsToH3(
  fileName,
  resolution,
  batchSizeDefine,
  batchSizeInsert,
  fileConvertUrl,
  tableName,
  databaseUrl,
  convertedJSONFolder
) {
  var hrstart = process.hrtime()

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
    console.log('Define completed with optResolution: ' + optResolution)

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
    console.log('TIME -------------------------------------')
    var hrend = process.hrtime(hrstart)
    console.info('Execution time (hr): %ds %dms', hrend[0], hrend[1] / 1000000)

    const db = await open({
      filename: databaseUrl,
      driver: sqlite3.Database,
    })

    await db.run(`DROP TABLE IF EXISTS ${tableName}`)
    await db.run(`CREATE TABLE ${tableName}(${tableFieldsTypesString})`)
    await db.close()

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
        // let timeStart = process.hrtime()
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
        // console.log('CHUNK TIME -------------------------------------')
        // let timeEnd = process.hrtime(timeStart)
        // console.info(
        //   'Execution time (hr): %ds %dms',
        //   timeEnd[0],
        //   timeEnd[1] / 1000000
        // )
        return valuesSql
      },
      new batch({ batchSize: batchSizeInsert }),
    ])
    await pipeline.on('data', (data) => {
      data.forEach(async (query, i) => {
        try {
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
        h3toGeoJsonFile(databaseUrl, tableName, convertedJSONFolder)
      } catch (error) {
        console.log(error)
      }
      console.log('Database is closed')
      return true
    })
  })
}

// PURE FUNCTIONS
// ====================================================================

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
  // loop features
  const h3indexesArrayNested = features.map((feature, fid) => {
    if (feature.value.geometry.type === 'MultiPolygon') {
      console.log(feature.value.geometry.type)
      let H3indexes = convertMultiPolygonFeatureToH3indexes(
        feature,
        resolution,
        attributes,
        fid
      )
      return H3indexes
    } else {
      console.log(feature.value.geometry.type)
      let H3indexes = convertPolygonFeatureToH3indexes(
        feature,
        resolution,
        attributes,
        fid
      )
      return H3indexes
    }
  })
  const h3indexesArrayFlattened = [].concat.apply([], h3indexesArrayNested)
  return {
    hex: h3indexesArrayFlattened,
    hexAllQuantity: h3indexesArrayFlattened.length,
  }
}

// convert polygon feature to H3 indexes inside polygon
function convertPolygonFeatureToH3indexes(
  feature,
  resolution,
  attributes,
  fid
) {
  const featureProperties = feature.value.properties
  const polygonCoordinates = feature.value.geometry.coordinates[0]
  const resolutionInteger = parseInt(resolution)
  const polygonH3indexes = polyfill(polygonCoordinates, resolutionInteger, true)
  // loop hexagons inside feature to add attributes
  const polygonH3indexesAttributes = polygonH3indexes.map((hexIndex) => {
    let H3indexAttributes = { H3INDEX: hexIndex, FID: fid + 1 }
    if (attributes === 'attributes') {
      Object.entries(featureProperties).forEach(([key, value]) => {
        H3indexAttributes[key] = value
      })
    }
    return H3indexAttributes
  })
  return polygonH3indexesAttributes
}

// convert polygon feature to H3 indexes inside polygon
function convertMultiPolygonFeatureToH3indexes(
  feature,
  resolution,
  attributes,
  fid
) {
  const featureProperties = feature.value.properties
  const polygons = feature.value.geometry.coordinates
  const resolutionInteger = parseInt(resolution)
  const h3indexesArrayNested = polygons.map((polygon) => {
    const polygonH3indexes = polyfill(polygon, resolutionInteger, true)
    return polygonH3indexes
  })
  const h3indexesArrayNestedNotEmpty = h3indexesArrayNested.filter((item) => {
    return item.length > 0
  })
  const h3indexesArrayFlattened = [].concat.apply(
    [],
    h3indexesArrayNestedNotEmpty
  )

  // loop hexagons inside feature to add attributes
  const polygonH3indexesAttributes = h3indexesArrayFlattened.map((hexIndex) => {
    let H3indexAttributes = { H3INDEX: hexIndex, FID: fid + 1 }
    if (attributes === 'attributes') {
      Object.entries(featureProperties).forEach(([key, value]) => {
        H3indexAttributes[key] = value
      })
    }
    return H3indexAttributes
  })
  return polygonH3indexesAttributes
}

// selection lowest resolution among highest hexs quantity
function optimalResolution(array) {
  const maxUniqHexQuantity = maxBy(array, 'uniqhexs')
  const maxUiqHexs = filter(array, ['uniqhexs', maxUniqHexQuantity.uniqhexs])
  const minResolutionLevel = minBy(maxUiqHexs, 'level')
  return minResolutionLevel.level
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
    // optimal resolution calculation
    console.log('input resolution')
    let resolution = 6
    let hexagons = geoJsonFeaturesToH3Stream(
      features,
      resolution,
      'attributes'
    ).hex
    return {
      hexagons: hexagons,
      optResolution: res,
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
      console.log('Object: ' + value)
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
