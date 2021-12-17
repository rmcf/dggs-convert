import fs from 'fs'
import sqlite3 from 'sqlite3'
import { open } from 'sqlite'
import { geoToH3 } from 'h3-js'
// import { h3SetToFeatureCollection } from 'geojson2h3'
sqlite3.verbose()

async function mainApp() {
  // conlsole arguments
  const args = process.argv.slice(2)

  // get features form geoJSON
  var features = null
  try {
    const data = fs.readFileSync('./files/data.geojson', 'utf8')
    // parse JSON string to JSON object
    const dataJSON = JSON.parse(data)
    features = dataJSON.features
  } catch (err) {
    console.log(`Error reading file from disk: ${err}`)
  }

  const hexagons = features.map((feature) => {
    let long = feature.geometry.coordinates[0]
    let lat = feature.geometry.coordinates[1]
    let h3Index = geoToH3(lat, long, 5)
    let hexagon = { H3INDEX: h3Index }
    Object.entries(feature.properties).forEach(([key, value]) => {
      hexagon[key] = value
    })
    return hexagon
  })

  // convert H3 indexes to geoJSON features
  // ===========================================================
  // const filename = args[0]
  // const filePath = './files/' + filename + '.geojson'
  // let hexIDs = hexagons.map((item) => item.id)
  // let geoFeatures = h3SetToFeatureCollection(hexIDs)
  // fs.writeFile(filePath, JSON.stringify(geoFeatures), function (err) {
  //   if (err) {
  //     return console.log(err)
  //   }
  // })

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
    filename: './databases/dggs.db',
    driver: sqlite3.Database,
  })

  // remove table if exists
  await db.run('DROP TABLE IF EXISTS groups')

  // create table
  await db.run(`CREATE TABLE groups(${tableFieldsTypesString})`)

  // insert data to table
  hexagons.forEach((hex) => {
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
    var sql = `INSERT INTO groups (${tableFieldsNamesString}) VALUES (${hexValuesString})`
    db.run(sql, [], function (err) {
      if (err) {
        return console.error(err.message)
      }
      console.log(`Rows inserted ${this.changes}`)
    })
  })

  // query data to check
  let query2 = `SELECT rowid, H3INDEX, LATITUDE, LONGITUDE, YEAR FROM groups`
  await db.each(query2, [], (err, row) => {
    if (err) {
      throw err
    }
    console.log(
      `${row.rowid} ${row.H3INDEX} ${row.YEAR} ${row.LATITUDE} ${row.LONGITUDE} `
    )
  })

  // close db connection
  await db.close()
}

// run app
mainApp()
