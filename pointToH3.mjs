import fs from 'fs'
import sqlite3 from 'sqlite3'
import { open } from 'sqlite'
import { geoToH3 } from 'h3-js'
import { h3SetToFeatureCollection } from 'geojson2h3'
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

  // convert features to H3 indexes
  var hexagons = []
  features.forEach((feature) => {
    let long = feature.geometry.coordinates[0]
    let lat = feature.geometry.coordinates[1]
    let h3Index = geoToH3(lat, long, 5)
    let hexagon = { id: h3Index }
    Object.entries(feature.properties).forEach((entry) => {
      const [key, value] = entry
      hexagon[key] = value
    })
    hexagons.push(hexagon)
  })
  console.log(hexagons)

  // convert H3 indexes to geoJSON features
  const filename = args[0]
  const filePath = './files/' + filename + '.geojson'
  let hexIDs = hexagons.map((item) => item.id)
  let geoFeatures = h3SetToFeatureCollection(hexIDs)
  fs.writeFile(filePath, JSON.stringify(geoFeatures), function (err) {
    if (err) {
      return console.log(err)
    }
  })

  // db connection
  const db = await open({
    filename: './databases/dggs.db',
    driver: sqlite3.Database,
  })

  await db.close()
}

// run app
mainApp()
