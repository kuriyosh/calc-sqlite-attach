import Database from "better-sqlite3";
import { performance, PerformanceObserver } from "node:perf_hooks";
import { Readable } from "node:stream";
import { finished } from "node:stream/promises";
import { createWriteStream } from "node:fs";
import { format } from "@fast-csv/format";

// 単に 1 つのデータベースをマージするだけ
async function mergeAllInOneDatabase(numOfTables: number) {
  const allInOneConnection = new Database("./data/allInOne.sqlite");

  const stmt = allInOneConnection.prepare(
    `SELECT
    table0.patientId, ${Array(numOfTables)
      .fill(0)
      .map((_, i) => `table${i + 1}.column${i}`)
      .join(",")}
    FROM
    table0
    ${Array(numOfTables)
      .fill(0)
      .map(
        (_, i) =>
          `LEFT JOIN table${i + 1} ON table0.patientId = table${
            i + 1
          }.patientId`
      )
      .join("\n")}`
  );

  const readableStream = Readable.from(stmt.iterate());
  const writeStream = createWriteStream("./result/allInOneDatabase.csv");

  readableStream.pipe(format({ headers: true })).pipe(writeStream);

  await finished(readableStream);
}

async function mergeMultipleDatabase(numOfTables: number) {
  const multipleConnection = new Database("./data/multi_0.sqlite");

  for (let i = 1; i < 10; i++) {
    multipleConnection.exec(
      `ATTACH DATABASE './data/multi_${i}.sqlite' as db${i}`
    );
  }

  const stmt = multipleConnection.prepare(
    `SELECT
    main.dataTable.patientId, ${Array(numOfTables)
      .fill(0)
      .map((_, i) => `db${i + 1}.dataTable.column${i}`)
      .join(", ")}
    FROM
    dataTable
    ${Array(numOfTables)
      .fill(0)
      .map(
        (_, i) =>
          `LEFT JOIN db${i + 1}.dataTable ON main.dataTable.patientId = db${
            i + 1
          }.dataTable.patientId`
      )
      .join("\n")}`
  );

  const readableStream = Readable.from(stmt.iterate());
  const writeStream = createWriteStream("./result/multipleDatabase.csv");

  readableStream.pipe(format({ headers: true })).pipe(writeStream);

  await finished(readableStream);
}

async function main() {
  const perfObserver = new PerformanceObserver((items) => {
    items.getEntries().forEach((entry) => {
      console.log(entry); // fake call to our custom logging solution
    });
  });

  perfObserver.observe({ entryTypes: ["measure"] });

  const args = process.argv;
  const numOfTables = Number(process.argv[3]);

  performance.mark("start");

  if (args[2] === "multi") {
    await mergeMultipleDatabase(numOfTables);
  } else if (args[2] === "one") {
    await mergeAllInOneDatabase(numOfTables);
  } else {
    console.log("nothing to do");
  }

  performance.mark("end");
  performance.measure("result", "start", "end");
}

(async () => {
  await main();
})();
