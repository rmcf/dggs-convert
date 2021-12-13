import fs from 'fs'
import { h3SetToFeatureCollection } from 'geojson2h3'

export async function h3toGeoJsonFile(args, db, elevation, resolution) {
  let sql = `SELECT cell_id id,
    elevation elevation
    FROM srtm_30m_estonia_h3
    WHERE elevation > ? AND resolution = ?
    ORDER BY elevation`

  let params = [elevation, resolution]

  const filename = args[0]
  const filePath = './files/' + filename + '.geojson'

  const list = []
  const rowsCount = await db.each(sql, params, (err, row) => {
    if (err) {
      throw err
    }
    // row = { col: 'other thing' }
    list.push(row)
  })

  if (list.length > 0) {
    let hexIDs = list.map((item) => item.id)

    let geoFeatures = h3SetToFeatureCollection(hexIDs)

    fs.writeFile(filePath, JSON.stringify(geoFeatures), function (err) {
      if (err) {
        return console.log(err)
      }
    })

    console.log('Created file with ' + list.length + ' features')
  } else console.log('No features created')

  return list
}
