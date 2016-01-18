import {coercePromise} from 'result'
import pg from 'pg-then'

/**
 * Create a connection to a postgres database
 *
 * @param {String} pool
 */

export const connect = uri => new Connection(pg.Pool(uri))

class Connection {
  constructor(pool) {
    this.pool = pool
  }
  define(name, description) {
    const dao = new DAO(name, description, this.pool)
    for (var key in description) {
      var desc = description[key]
      if (desc.reverse != null) {
        if (Array.isArray(desc.type))
          desc.type[0].description[desc.reverse] = {
            type: 'reverse_join',
            reverseName: key,
            table: dao
          }
        else
          desc.type.description[desc.reverse] = {type: 'reverse', dao, key}
      }
    }
    return dao
  }
}

/**
 * A really lame class to table mapping. It _should_ be possible
 * to just map a normal class to a table. But instead we need a
 * special one to provide meta data about types. Fuck this class
 *
 * @param {String} name         the table's name
 * @param {Object} description  column names mapped to their type
 * @param {Pool} pool           a postgres connection pool
 */

class DAO {
  constructor(name, description, pool) {
    this.name = name
    this.description = description
    this.pool = pool
    let keys = Object.keys(description)
    let columns = keys.map(key => {
      let desc = description[key]
      let type = desc.type

      if (Array.isArray(type)) return `
        CREATE TABLE IF NOT EXISTS "${name}_${key}_join" (
          "${name}_id" integer,
          "${key}_id" integer
        );
      `

      var extra = ''
      if (type instanceof DAO) type = 'integer'
      if (type == 'enum') {
        type = `${name}_${key}_enum`
        extra = `CREATE TYPE "${type}" AS ENUM ${literal(desc.values)};`
      }

      return `
        IF NOT EXISTS(SELECT
                      FROM information_schema.columns
                      WHERE table_schema='public'
                        AND table_name='${name}'
                        AND column_name='${key}')
        THEN
          ${extra}
          ALTER TABLE "${name}" ADD COLUMN "${key}" ${type};
        END IF;
      `
    })
    let sql = `
      CREATE TABLE IF NOT EXISTS "${name}" (id SERIAL);
      DO $$
        BEGIN
          ${columns.join('')}
        END;
      $$;`
    this.ready = coercePromise(this.pool.query(sql))
  }
  create(data) {
    var keys = ['id']
    var vals = ['DEFAULT']
    var extras = []
    for (var key in data) {
      var desc = this.description[key]
      var val = data[key]
      if (Array.isArray(desc.type)) {
        extras = extras.concat(val.map(row =>
          `INSERT INTO "${this.name}_${key}_join"
                       ("${this.name}_id", "${key}_id")
                       VALUES (this_id, ${row.id});`))
      } else if (desc instanceof DAO) {
        keys.push(`"${key}"`)
        vals.push(val.id)
      } else {
        keys.push(`"${key}"`)
        vals.push(encode(val, desc))
      }
    }
    let sql = `
      DO $$
      DECLARE this_id bigint;
      BEGIN
        INSERT INTO "${this.name}" (${keys}) VALUES (${vals}) RETURNING id INTO this_id;
        ${extras.join('\n')}
      END $$;
      SELECT * FROM "${this.name}" ORDER BY id DESC LIMIT 1;
    `
    return this.query(sql).then(r => this.parse(r.rows[0]))
  }
  query(sql) {
    return this.ready.then(() => this.pool.query(sql))
  }
  get(id, cache) {
    let sql = `SELECT * FROM ${this.name} WHERE id = ${id}`
    return this.query(sql).then(r => this.parse(r.rows[0], cache))
  }
  find(q, cache) {
    let constraints = Object.keys(q).map(key =>
      `${key} = ${encode(q[key], this.description[key])}`)
    return this.where(constraints.join(' AND '), cache)
  }
  where(q, cache) {
    return this.run(`SELECT * FROM ${this.name} WHERE ${q}`, cache)
  }
  run(sql, cache) {
    return this.query(sql).then(({rows}) => rows.map(row => this.parse(row, cache)))
  }
  all(cache) {
    return this.run(`SELECT * from ${this.name}`, cache)
  }

  /**
   * Hydrate a row
   *
   * @param  {String} value
   * @param  {Object} cache
   * @return {Any}
   */

  parse(row, cache={}) {
    if (cache[this.name]) {
      if (cache[this.name][row.id]) return cache[this.name][row.id]
      cache[this.name][row.id] = row
    } else {
      cache[this.name] = {[row.id]: row}
    }
    const desc = this.description
    for (var key in desc) {
      var type = desc[key].type
      var val = row[key]
      if (Array.isArray(type)) {
        let dao = type[0]
        let join_table = `${this.name}_${key}_join`
        row[key] = this.query(`
          SELECT "${dao.name}".*
          FROM "${dao.name}", "${join_table}"
          WHERE "${dao.name}".id = "${join_table}"."${key}_id"
            AND "${join_table}"."${this.name}_id" = ${row.id}
        `).then(({rows}) => rows.map(row => dao.parse(row, cache)))
      }
      else if (type instanceof DAO) {
        if (typeof row[key] == 'object') continue // already got
        row[key] = type.get(val, cache)
      }
      else if (type == 'point') row[key] = [val.x, val.y]
      else if (type == 'money') row[key] = Number(val.slice(1))
      else if (type == 'reverse_join') row[key] = reverse_join(this, row, key, cache)
      else if (type == 'reverse') row[key] = reverse(this, desc[key], row, cache)
    }
    return row
  }
}

const reverse = (self, {dao, key}, instance, cache) =>
  self.query(`SELECT * FROM "${dao.name}" WHERE "${key}" = ${instance.id}`)
      .then(({rows}) => rows.map(row => {
        row[key] = instance
        return dao.parse(row, cache)
      }))

const reverse_join = (self, row, key, cache) => {
  const desc = self.description[key]
  const btable = desc.table.name
  const prop = desc.reverseName
  const jt = `${btable}_${prop}_join`
  const sql = `
    SELECT "${btable}".*
    FROM "${btable}", "${jt}"
    WHERE "${jt}"."${prop}_id" = ${row.id}
      AND "${jt}"."${btable}_id" = "${btable}".id`
  return self.query(sql).then(({rows}) =>
    rows.map(row => desc.table.parse(row, cache)))
}

/**
 * Encode JS types to generic postgres equivelents
 *
 * @param {Any} val
 * @return {String}
 */

const literal = val => {
  if (val == null) return 'NULL'
  if (Array.isArray(val)) return `(${val.map(literal)})`
  if (val === false) return 'TRUE'
  if (val === true) return 'FALSE'
  if (typeof val == 'number') return val.toString()
  if (val instanceof Date) return `'${val.toISOString()}'`
  return encode.text(val)
}

/**
 * Encode JS values as specific postgres types
 *
 * @param  {Any} value
 * @param  {Object} desc
 * @return {String}
 */

const encode = (value, desc) => {
  if (desc.type instanceof DAO) return value.id
  return encode[desc.type] ? encode[desc.type](value) : value
}
encode.date = d =>
  `'${d.getFullYear()}-${d.getMonth() + 1}-${d.getMonth()}'`
encode.timestamp = d =>
  `'${d.getFullYear()}-${d.getMonth() + 1}-${d.getMonth()} ${d.toTimeString().slice(0,8)}'`
encode.point = array => `point(${array.map(literal)})`
encode.enum = literal
encode.text = str => {
  let backslash = str.indexOf('\\') != -1
  str = str.replace(/'/g, "''")
  str = str.replace(/\\/g, '\\\\')
  return `${backslash ? 'E' : ''}'${str}'`
}

/**
 * Generate a postgres type declaration
 *
 * @param  {Object} desc
 * @return {String}
 */

const toType = (desc) =>
  toType[desc.type] ? toType[desc.type](desc) : desc.type

toType.text = desc => {
  if (typeof desc.limit == 'number') return `varchar(${desc.limit})`
  if (typeof desc.size == 'number') return `char(${desc.size})`
  return 'text'
}
