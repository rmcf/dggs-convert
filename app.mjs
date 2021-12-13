import sqlite3 from 'sqlite3'
import { open } from 'sqlite'
// import { h3toGeoJsonFile } from './h3ToGeoJSONfile.mjs'
sqlite3.verbose()

async function mainApp() {
  // colsole arguments
  const args = process.argv.slice(2)

  // db connection
  const db = await open({
    filename: './databases/dggs.db',
    driver: sqlite3.Database,
  })

  // h3 to features based on "elevation" and "level"
  // let list = await h3toGeoJsonFile(args, db, 265, 8)
  // console.log(list)

  await db.close()
}

// run app
mainApp()
