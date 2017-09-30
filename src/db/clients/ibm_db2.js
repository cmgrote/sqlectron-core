import { Database } from 'ibm_db';
import { identify } from 'sql-query-identifier';

import { buildDatabseFilter, buildSchemaFilter } from './utils';
import createLogger from '../../logger';

const logger = createLogger('db:clients:ibm_db2');

export default function (server, database) {
  return new Promise(async (resolve, reject) => {
    const dbConfig = configDatabase(server, database);

    logger().debug('creating database client for ibm_db2 with config %j', dbConfig);
    const dbConnection = new Database();

    const defaultSchema = dbConfig.schema;

    logger().debug('connecting');
    dbConnection.open(dbConfig.connString, function(err, conn) {
      if (err) {
        return reject(Error(err));
      }

      logger().debug('connected');
      resolve({
        wrapIdentifier,
        disconnect: () => disconnect(dbConnection),
        listTables: (db, filter) => listTables(dbConnection, filter),
        listViews: (filter) => listViews(dbConnection, filter),
        listRoutines: () => listRoutines(dbConnection),
        listTableColumns: (db, table, schema = defaultSchema) => listTableColumns(dbConnection, db, table, schema),
        listTableTriggers: (table) => listTableTriggers(dbConnection, table),
        listTableIndexes: (db, table) => listTableIndexes(dbConnection, table),
        listSchemas: () => listSchemas(dbConnection),
        getTableReferences: (table) => getTableReferences(dbConnection, table),
        getTableKeys: (db, table) => getTableKeys(dbConnection, db, table),
        query: (queryText) => query(dbConnection, queryText),
        executeQuery: (queryText) => executeQuery(dbConnection, queryText),
        listDatabases: () => listDatabases(dbConfig),
        getQuerySelectTop: (table, limit) => getQuerySelectTop(dbConnection, table, limit),
        getTableCreateScript: (table) => getTableCreateScript(dbConnection, table),
        getViewCreateScript: (view) => getViewCreateScript(dbConnection, view),
        getRoutineCreateScript: (routine) => getRoutineCreateScript(dbConnection, routine),
        truncateAllTables: (db) => truncateAllTables(dbConnection, db),
      });
    });
  });
}


export function disconnect(connection) {
  connection.closeSync();
}


export function listTables(connection, filter) {
  const schemaFilter = buildSchemaFilter(filter, 'creator');
  return new Promise((resolve, reject) => {
    const sql = `
      SELECT
        creator,
        name
      FROM sysibm.systables
      WHERE type = 'T'
      ${schemaFilter ? `AND ${schemaFilter}` : ''}
      ORDER BY creator, name
    `;
    const params = [];
//    logger().debug('listing tables with: ' + sql);
    connection.query(sql, params, (err, data) => {
      if (err) return reject(Error(err));
//      logger().debug('got tables: %j', data);
//      logger().debug('resolving with: %j', data.map((row) => ({ name: row.NAME })));
      resolve(data.map((item) => ({
        schema: item.CREATOR,
        name: item.NAME,
      })));
    });
  });
}

export function listViews(connection, filter) {
  const schemaFilter = buildSchemaFilter(filter, 'creator');
  return new Promise((resolve, reject) => {
    const sql = `
      SELECT 
        creator,
        name
      FROM sysibm.systables
      WHERE type = 'V'
      ${schemaFilter ? `AND ${schemaFilter}` : ''}
      ORDER BY creator, name
    `;
    const params = [];
//    logger().debug('listing views with: ' + sql);
    connection.query(sql, params, (err, data) => {
      if (err) return reject(Error(err));
//      logger().debug('got views: %j', data);
//      logger().debug('resolving with: %j', data.map((row) => ({ name: row.NAME })));
      resolve(data.map((item) => ({
        schema: item.CREATOR,
        name: item.NAME,
      })));
    });
  });
}

export function listRoutines() {
  return Promise.resolve([]);
}

export function listTableColumns(connection, database, table, schema) {
  return new Promise((resolve, reject) => {
    const sql = `
      SELECT colno as position, colname as column_name, typename as type
      FROM syscat.columns
      WHERE tabschema = ?
        AND tabname = ?
    `;
    const params = [
      schema,
      table,
    ];
//    logger().debug('listing table columns with: ' + sql);
    connection.query(sql, params, (err, data) => {
      if (err) return reject(Error(err));
      resolve(data.map((row) => ({
          columnName: row.COLUMN_NAME,
          dataType: row.TYPE,
        }))
      );
    });
  });
}

export function listTableTriggers() {
  return Promise.resolve([]);
}
export function listTableIndexes() {
  return Promise.resolve([]);
}

export function listSchemas(connection) {
  return new Promise((resolve, reject) => {
    const sql = 'SELECT RTRIM(schemaname) as name FROM syscat.schemata';
    const params = [];
//    logger().debug('listing schemas with: ' + sql);
    connection.query(sql, params, (err, data) => {
      if (err) return reject(err);
//      logger().debug('got schemas: %j', data);
//      logger().debug('resolving with: %j', data.map((row) => row.NAME));
      resolve(data.map((row) => row.NAME));
    });
  });
}

export function getTableReferences() {
  return Promise.resolve([]);
}

export function getTableKeys(connection, database, table) {
  return new Promise((resolve, reject) => {
    const sql = `
      SELECT colname as column_name
      FROM syscat.columns
      WHERE tabschema = ?
        AND tabname = ?
        AND keyseq IS NOT NULL
        AND keyseq > 0
    `;
    const params = [
      database,
      table,
    ];
//    logger().debug('listing table keys with: ' + sql);
    connection.query(sql, params, (err, data) => {
      if (err) return reject(Error(err));
      resolve(data.map((row) => ({
        constraintName: null,
        columnName: row.COLUMN_NAME,
        referencedTable: null,
        keyType: 'PRIMARY KEY',
      })));
    });
  });
}

function query(conn, queryText) { // eslint-disable-line no-unused-vars
  throw new Error('"query" function is not implementd by ibm_db2 client.');
}

export async function executeQuery(conn, queryText) {

  const commands = identifyCommands(queryText).map((item) => item.type);

  return new Promise(function(resolve, reject) {
//    logger().debug('running arbitrary query: ' + queryText);
    conn.query(queryText, function(errQuery, rows) {
      if (errQuery) reject(Error(errQuery));
//      logger().debug('untouched query result = %j', rows);
      if (!Array.isArray(rows[0])) {
        rows = [rows];
      }
//      logger().debug('pre-resolution query result = %j', rows);
      resolve(rows.map((resultSet, idx) => parseRowQueryResult(resultSet, commands[idx])));
    });
  });

}


export function listDatabases(dbConfig) {
  return Promise.resolve([dbConfig.database]);
}


export function getQuerySelectTop(client, table, limit) {
  return `SELECT * FROM ${wrapIdentifier(table)} LIMIT ${limit}`;
}

export function getTableCreateScript() {
  return Promise.resolve([]);
}

export function getViewCreateScript() {
  return Promise.resolve([]);
}

export function getRoutineCreateScript() {
  return Promise.resolve([]);
}

export function wrapIdentifier(value) {
  if (value === '*') return value;
  const matched = value.match(/(.*?)(\[[0-9]\])/); // eslint-disable-line no-useless-escape
  if (matched) return wrapIdentifier(matched[1]) + matched[2];
  return `"${value.replace(/"/g, '""')}"`;
}


// TODO: implement...
export const truncateAllTables = async (connection, database) => {
  const sql = `
    SELECT name
    FROM sysibm.systables
    WHERE type = 'T'
    AND creator = '${database}'
  `;
  const [result] = await executeQuery(connection, sql);
  const tables = result.rows.map((row) => row.table_name);
  const promises = tables.map((t) => {
    const truncateSQL = `
      TRUNCATE TABLE ${wrapIdentifier(database)}.${wrapIdentifier(t)};
    `;
    return executeQuery(connection, truncateSQL);
  });

  await Promise.all(promises);
};

function configDatabase(server, database) {

  const config = {
    database: database.database,
    dbhost: server.config.host,
    dbport: server.config.port,
    user: server.config.user,
    password: server.config.password,
    schema: database.schema,
    connString: ""
      + "DATABASE=" + database.database + ";"
      + "HOSTNAME=" + server.config.host + ";"
      + "PORT=" + server.config.port +";"
      + "PROTOCOL=TCPIP;"
      + "UID=" + server.config.user + ";"
      + "PWD=" + server.config.password + ";"
  };

  // TODO: provide a way of specifying the server's certificate file
  // ;SSLServerCertificate=<armFilePath>;
  // (below with just SSL is still useful for connecting to dashDB in Bluemix)
  if (server.config.ssl) {
    config.connString += "Security=SSL";
  }

  if (server.sshTunnel) {
    config.server = server.config.localHost;
    config.port = server.config.localPort;
  }

  return config;
}


function parseRowQueryResult(data, command) {
//  logger().debug('Parsing (' + command + '): %j', data);
  const obj = {
    command: command,
    rows: data || [],
    fields: Object.keys(data[0] || {}).map((name) => ({ name })),
    rowCount: data.length,
    affectedRows: data.length,
  };
//  logger().debug(' ... returning: %j', obj);
  return obj;
}


function identifyCommands(queryText) {
  try {
    return identify(queryText);
  } catch (err) {
    return [];
  }
}
