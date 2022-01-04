import fs from 'fs'
import sqlite3 from 'sqlite3'
import { open } from 'sqlite'
import { geoToH3 } from 'h3-js'
import { h3SetToFeatureCollection, h3ToFeature } from 'geojson2h3'
import maxBy from 'lodash.maxby'
import minBy from 'lodash.minby'
import filter from 'lodash.filter'
sqlite3.verbose()

// formatting numbers to string ("02")
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
function geoJsonFeaturesToH3(features, resolution, attributes) {
  let h3IndexesArray = []
  let hexagons = features.map((feature) => {
    let long = feature.geometry.coordinates[0]
    let lat = feature.geometry.coordinates[1]
    let h3Index = geoToH3(lat, long, resolution)
    h3IndexesArray.push(h3Index)
    let hexagon = { H3INDEX: h3Index }
    if (attributes === 1) {
      Object.entries(feature.properties).forEach(([key, value]) => {
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

// save H3 indexes as hexagons with attributes to geojson
function h3toGeoJsonAttributes(indexes) {
  let hexagons = []
  indexes.forEach((index) => {
    // vector hexagon on the map
    let hexagon = h3ToFeature(index.H3INDEX)
    // assign all attributes of featureFromAPI to vector hexagon
    Object.entries(index).forEach((entry) => {
      const [key, value] = entry
      if (key !== 'H3INDEX') {
        hexagon.properties[key] = value
      }
    })
    hexagons.push(hexagon)
  })
  let geoJsonObject = {
    type: 'FeatureCollection',
    features: hexagons,
  }
  const filePath = './files/resultGeojson/' + outComeFilename()
  fs.writeFile(filePath, JSON.stringify(geoJsonObject), function (err) {
    if (err) {
      return console.log(err)
    }
  })
}

// conlsole arguments
const args = process.argv.slice(2)
const inComeFilename = args[0]
const inComeResolution = () => {
  if (args[1] !== undefined) {
    return args[1]
  } else {
    return null
  }
}
const outComeFilename = () => {
  if (args[2] !== undefined) {
    return args[2]
  } else {
    return null
  }
}

// main function
async function mainApp() {
  console.time('time')

  // get features form geoJSON
  var features = null
  try {
    const data = fs.readFileSync(
      './files/geoJsonToConvert/' + inComeFilename,
      'utf8'
    )
    // parse JSON string to JSON object
    const dataJSON = JSON.parse(data)
    features = dataJSON.features
    const crs = dataJSON.crs
  } catch (err) {
    console.log(`Error reading file from disk: ${err}`)
  }

  // verification features for coordinates duplicates
  const coordinatesString = features.map((feature) => {
    let long = feature.geometry.coordinates[0]
    let lat = feature.geometry.coordinates[1]
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

  // array of H3 indexes with attributes
  var hexagons = null
  if (inComeResolution()) {
    let resolution = inComeResolution()
    hexagons = geoJsonFeaturesToH3(features, resolution, 1).hex
  } else {
    var startResolution = 1
    var conversionResult = {} // result of conversion function
    var uniqHexAtLevel = [] // hexagon quantity at each resolution level
    do {
      conversionResult = geoJsonFeaturesToH3(features, startResolution, 0)
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
    console.log('-------------------------------------')
    console.log('Optimal resolution:')
    hexagons = geoJsonFeaturesToH3(features, optResolution, 1).hex
  }

  // convert H3 indexes to geoJSON features
  if (outComeFilename()) {
    h3toGeoJsonAttributes(hexagons)
  }

  // define fields names and types for sqlite table
  const hexFirst = hexagons[0]
  var tableFieldsNames = []
  var tableFieldsTypes = []
  // define types of each property of hexagon
  Object.entries(hexFirst).forEach(([key, value]) => {
    let field = null
    let hexagon = hexagons.find((hex) => hex[key] !== null)
    if (hexagon === undefined) {
      field = `${key} TEXT`
    } else {
      let fieldTypeOf = typeof value
      let fieldType = () => {
        if (fieldTypeOf === 'string') {
          return 'TEXT'
        } else {
          if (fieldTypeOf === 'number') {
            if (Number.isInteger(value)) {
              return 'INTEGER'
            } else {
              return 'REAL'
            }
          } else {
            if (fieldTypeOf === 'boolean') {
              return 'TEXT'
            } else {
              return 'TEXT'
            }
          }
        }
      }
      field = `${key} ${fieldType()}`
    }
    tableFieldsNames.push(key)
    tableFieldsTypes.push(field)
  })

  const tableFieldsNamesString = tableFieldsNames.join(',')
  const tableFieldsTypesString = tableFieldsTypes.join(', ')

  // db connection
  const db = await open({
    filename: './db/dggs.db',
    driver: sqlite3.Database,
  })
  console.log('Started recording to database')
  console.log('...')

  // remove table if exists
  await db.run('DROP TABLE IF EXISTS groups')

  // create table
  await db.run(`CREATE TABLE groups(${tableFieldsTypesString})`)

  // insert data to table
  // hexagons.forEach((hex) => {
  //   let hexValues = []
  //   Object.entries(hex).forEach(([key, value]) => {
  //     let valueText = null
  //     if (value === null) {
  //       valueText = 'null'
  //     } else {
  //       if (typeof value === 'string') {
  //         valueText = '"' + value + '"'
  //       } else valueText = value
  //     }
  //     hexValues.push(valueText)
  //   })
  //   var hexValuesString = hexValues.join(', ')
  //   var sql = `INSERT INTO groups (${tableFieldsNamesString}) VALUES (${hexValuesString})`
  //   db.run(sql, [], function (err) {
  //     if (err) {
  //       return console.error(err.message)
  //     }
  //     console.log(`Rows inserted ${this.changes}`)
  //   })
  // })

  // query data to check
  // let query2 = `SELECT rowid, H3INDEX, LATITUDE, LONGITUDE, YEAR FROM groups`
  // await db.each(query2, [], (err, row) => {
  //   if (err) {
  //     throw err
  //   }
  //   console.log(
  //     `${row.rowid} ${row.H3INDEX} ${row.YEAR} ${row.LATITUDE} ${row.LONGITUDE} `
  //   )
  // })

  // close db connection
  await db.close()
  console.log('Finished recording to database')
  console.timeEnd('time')
}

// run app
mainApp()
