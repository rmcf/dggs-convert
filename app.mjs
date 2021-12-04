import fs from 'fs'
import sqlite3 from 'sqlite3'
import { open } from 'sqlite'
import { h3SetToFeatureCollection } from 'geojson2h3'
sqlite3.verbose()

async function mainApp() {
  // progress bar
  // for (let i = 0; i <= 20; i++) {
  //   const dots = '.'.repeat(i)
  //   const left = 20 - i
  //   const empty = ' '.repeat(left)
  //   process.stdout.write(`\r[${dots}${empty}] ${i * 5}% `)

  const args = process.argv.slice(2)
  const filename = args[0]
  const filePath = './files/' + filename + '.geojson'

  // main functions
  // ===================================================
  const db = await open({
    filename: './databases/dggs.db',
    driver: sqlite3.Database,
  })

  let sql = `SELECT cell_id id,
              elevation elevation
              FROM srtm_30m_estonia_h3
              WHERE elevation > ? AND resolution = ?
              ORDER BY elevation`

  let params = [240, 8]

  const list = []
  const rowsCount = await db.each(sql, params, (err, row) => {
    if (err) {
      throw err
    }
    // row = { col: 'other thing' }
    list.push(row)
  })

  let hexIDs = list.map((item) => item.id)
  let geoFeatures = h3SetToFeatureCollection(hexIDs)

  fs.writeFile(filePath, JSON.stringify(geoFeatures), function (err) {
    if (err) {
      return console.log(err)
    }
  })

  await db.close()
  // }
}

mainApp()
