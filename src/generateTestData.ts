import { v4 as uuidv4 } from "uuid";
import { randomBytes } from "node:crypto";
import Database from "better-sqlite3";

const ROWS = 1000_000;
const TABLES = 10;
const COLUMNS = 10;
const ALL_IN_ONE_DB_NAME = "allInOne.sqlite";
const MULTI_DB_PREFIX = "multi";

const allInOneConnection = new Database(`./data/${ALL_IN_ONE_DB_NAME}`);
const multiDbConnections: Database.Database[] = [];
for (let i = 0; i < TABLES; i++) {
  multiDbConnections.push(
    new Database(`./data/${MULTI_DB_PREFIX}_${i}.sqlite`)
  );
}

async function init() {
  const createTableStatement = (tableName: string) =>
    `CREATE TABLE IF NOT EXISTS ${tableName}('patientId' varchar, ${Array(
      COLUMNS - 1
    )
      .fill(0)
      .map((_, i) => `'column${i}' varchar`)
      .join(",")})`;

  for (let i = 0; i < TABLES; i++) {
    console.log(createTableStatement(`table${i}`));
    allInOneConnection.exec(createTableStatement(`table${i}`));
  }

  for (const connection of multiDbConnections) {
    connection.exec(createTableStatement("dataTable"));
  }
}

async function insertDatabase(rows: string[][]) {
  for (const [index, row] of rows.entries()) {
    allInOneConnection.exec(
      `INSERT INTO table${index} values(${row.map((v) => `'${v}'`).join(",")})`
    );
    multiDbConnections[index].exec(
      `INSERT INTO dataTable values(${row.map((v) => `'${v}'`).join(",")})`
    );
  }
}

async function main() {
  init();

  for (let i = 0; i < ROWS; i++) {
    const patientId = uuidv4();
    const tableData: string[][] = [];
    for (let j = 0; j < TABLES; j++) {
      const row = [patientId];
      for (let k = 1; k < COLUMNS; k++) {
        row.push(randomBytes(20).toString("hex").slice(0, 20));
      }
      tableData.push(row);
    }
    insertDatabase(tableData);
  }
}

(async () => {
  await main();
})();
