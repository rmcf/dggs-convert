import fs from 'fs'
import { h3ToFeature } from 'geojson2h3'
import sqlite3 from 'sqlite3'
sqlite3.verbose()

export async function h3toGeoJsonFile(
  databaseUrl,
  tableName,
  convertedJSONFolder
) {
  if (convertedJSONFolder.length === 0) {
    return false
  } else {
    const filePath = convertedJSONFolder + tableName + '.geojson'

    let sql = `SELECT * FROM ${tableName}`
    let params = []

    const db = new sqlite3.Database(databaseUrl)

    db.all(sql, params, (error, rows) => {
      if (error) {
        throw error
      } else {
        if (rows.length > 0) {
          let hexagons = []
          rows.forEach((index) => {
            let hexagon = h3ToFeature(index.H3INDEX)
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
          fs.writeFile(filePath, JSON.stringify(geoJsonObject), function (err) {
            if (err) {
              return console.log(err)
            }
          })
          console.log('Created file with ' + rows.length + ' features')
        } else console.log('No features created')
      }
    })

    db.close()
    return true
  }
}
