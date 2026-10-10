/* Real HTTP + PostgreSQL integration. Run only against the isolated local database. */
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { writeFileSync, mkdirSync } = require('node:fs');
const { resolve } = require('node:path');
const { Client } = require('pg');
const inventoryUrl = process.env.TEST_INVENTORY_URL || 'http://127.0.0.1:3311/kpi_inventory';
const maintenanceUrl = process.env.TEST_MAINTENANCE_URL || 'http://127.0.0.1:3312/kpi_maintenance';
if (![inventoryUrl, maintenanceUrl].every(url => new URL(url).hostname === '127.0.0.1')) throw new Error('Local APIs required');
const db = new Client({ host: '127.0.0.1', port: 55432, user: 'reception_test', database: 'reception_e2e' });
const code = `CROSS-${Date.now()}`;
const ids = Object.fromEntries(['sucursal', 'source', 'dest', 'product', 'stock', 'transfer', 'newDetail', 'usedDetail', 'wo'].map(name => [name, randomUUID()]));
const results = [];
const headers = { 'content-type': 'application/json', 'x-role-name': 'BODEGA', 'x-user-name': 'test-reception', 'x-user-display-name': 'Test recepción' };
async function http(base, path, method = 'GET', body) {
  const response = await fetch(`${base}${path}`, { method, headers, body: body ? JSON.stringify(body) : undefined });
  const data = await response.json();
  return { status: response.status, data: data.data ?? data };
}
async function test(name, fn) { await fn(); results.push({ name, passed: true }); console.log(`PASS ${name}`); }
async function stock() { return (await db.query('SELECT * FROM kpi_inventory.tb_stock_bodega WHERE id=$1', [ids.stock])).rows[0]; }
async function seedTransfer(product, source, dest, quantity, condition = 'NUEVO') {
  const transfer = randomUUID();
  await db.query(`INSERT INTO kpi_inventory.tb_transferencia_bodega(id,codigo,bodega_origen_id,bodega_destino_id,estado,recepcion_requerida,total_items,total_cantidad) VALUES($1,$2,$3,$4,'PENDIENTE_RECEPCION',true,1,$5)`, [transfer, `${code}-${condition}-${Math.random().toString(16).slice(2, 6)}`, source, dest, quantity]);
  await db.query(`INSERT INTO kpi_inventory.tb_transferencia_bodega_det(transferencia_bodega_id,producto_id,nombre_producto,cantidad,cantidad_recibida,condicion_material,bodega_origen_id,bodega_destino_id) VALUES($1,$2,'Material de prueba',$3,0,$4,$5,$6)`, [transfer, product, quantity, condition, source, dest]);
  return transfer;
}
async function main() {
  await db.connect();
  assert.equal((await db.query('SELECT current_database() AS db')).rows[0].db, 'reception_e2e');
  assert.ok((await db.query("SELECT to_regclass('kpi_inventory.v_transferencia_stock_pendiente') AS view")).rows[0].view, 'Apply reception migration first');
  await db.query('INSERT INTO kpi_inventory.tb_sucursal(id,codigo,nombre) VALUES($1,$2,$3)', [ids.sucursal, code, 'Sucursal pruebas recepción']);
  for (const name of ['source', 'dest']) await db.query('INSERT INTO kpi_inventory.tb_bodega(id,sucursal_id,codigo,nombre) VALUES($1,$2,$3,$4)', [ids[name], ids.sucursal, `${code}-${name}`, name]);
  await db.query('INSERT INTO kpi_inventory.tb_producto(id,codigo,nombre,ultimo_costo,costo_promedio) VALUES($1,$2,$3,10,10)', [ids.product, code, 'Filtro pruebas recepción']);
  await db.query('INSERT INTO kpi_inventory.tb_stock_bodega(id,bodega_id,producto_id,stock_actual,stock_fisico,stock_nuevo,stock_usado,es_usado,costo_promedio_bodega) VALUES($1,$2,$3,20,20,12,8,true,10)', [ids.stock, ids.source, ids.product]);
  ids.newTransfer = await seedTransfer(ids.product, ids.source, ids.dest, 7);
  ids.usedTransfer = await seedTransfer(ids.product, ids.source, ids.dest, 4, 'USADO');
  await db.query("INSERT INTO kpi_process.tb_work_order(id,code,type,title,status_workflow,created_by) VALUES($1,$2,'MANTENIMIENTO','OT pruebas recepción','PLANNED','test-reception')", [ids.wo, code]);

  await test('stock listado y catálogo conservan registrado y excluyen pendiente por condición', async () => {
    for (const path of [`/stock-bodega?bodega_id=${ids.source}`, `/stock-bodega/catalogo?bodega_id=${ids.source}`]) {
      const response = await http(inventoryUrl, path);
      assert.equal(response.status, 200, JSON.stringify(response));
      const rows = Array.isArray(response.data) ? response.data : response.data.data;
      const row = rows.find(row => row.producto_id === ids.product);
      assert.equal(Number(row.stock_actual), 20);
      assert.equal(row.cantidad_pendiente_transferencia, 11);
      assert.equal(row.stock_disponible_nuevo, 5); assert.equal(row.stock_disponible_usado, 4);
    }
  });
  await test('reserva OT permite registrar necesidad; stock CRUD no puede borrar compromiso', async () => {
    const response = await http(maintenanceUrl, `/work-orders/${ids.wo}/consumos`, 'POST', { producto_id: ids.product, bodega_id: ids.source, cantidad: 8 });
    assert.equal(response.status, 201, JSON.stringify(response));
    assert.equal(response.data.cantidad_pendiente_transferencia, 11);
    assert.equal(response.data.stock_disponible_nuevo, 5);
    const update = await http(inventoryUrl, `/stock-bodega/${ids.stock}`, 'PATCH', { stock_nuevo: 6, stock_usado: 8, es_usado: true });
    assert.equal(update.status, 409, JSON.stringify(update));
    assert.equal(Number((await stock()).stock_actual), 20);
    const deletion = await http(inventoryUrl, `/stock-bodega/${ids.stock}`, 'DELETE');
    assert.equal(deletion.status, 409, JSON.stringify(deletion));
  });
  await test('OT rechaza material pendiente sin cambiar stock, reserva o Kardex', async () => {
    const response = await http(maintenanceUrl, `/work-orders/${ids.wo}/issue-materials`, 'POST', { items: [{ producto_id: ids.product, bodega_id: ids.source, cantidad: 6, condicion_material: 'NUEVO' }] });
    assert.equal(response.status, 409, JSON.stringify(response));
    assert.match(JSON.stringify(response.data), /pendiente de recepción/);
    assert.equal(Number((await stock()).stock_actual), 20);
    assert.equal((await db.query('SELECT count(*)::int AS count FROM kpi_inventory.tb_kardex WHERE producto_id=$1', [ids.product])).rows[0].count, 0);
  });
  await test('dos egresos concurrentes no consumen el material comprometido', async () => {
    const responses = await Promise.all([1, 2].map(() => http(maintenanceUrl, `/work-orders/${ids.wo}/issue-materials`, 'POST', { items: [{ producto_id: ids.product, bodega_id: ids.source, cantidad: 4, condicion_material: 'NUEVO' }] })));
    assert.deepEqual(responses.map(response => response.status).sort(), [201, 409], JSON.stringify(responses));
    assert.equal(Number((await stock()).stock_nuevo), 8);
    assert.equal(Number((await stock()).stock_usado), 8);
    const consumos = await http(maintenanceUrl, `/work-orders/${ids.wo}/consumos`);
    assert.equal(consumos.status, 200);
    assert.equal(consumos.data[0].stock_disponible_nuevo, 1);
    const reserved = await db.query("SELECT sum(cantidad)::numeric AS quantity FROM kpi_inventory.tb_reserva_stock WHERE work_order_id=$1 AND estado='RESERVADO'", [ids.wo]);
    assert.equal(Number(reserved.rows[0].quantity), 4);
  });
  await test('Kardex manual descuenta reservas OT más pendiente antes de egresar', async () => {
    const response = await http(inventoryUrl, '/kardex/documentos', 'POST', { tipo_movimiento: 'SALIDA', bodega_id: ids.source, detalles: [{ producto_id: ids.product, cantidad: 2, condicion_material: 'USADO' }] });
    assert.equal(response.status, 400, JSON.stringify(response));
    assert.match(JSON.stringify(response.data), /pendiente de recepción/);
    assert.equal(Number((await stock()).stock_usado), 8);
  });
  await test('anular ingreso manual comprometido en otra transferencia conserva inventario', async () => {
    const product = randomUUID();
    await db.query('INSERT INTO kpi_inventory.tb_producto(id,codigo,nombre,ultimo_costo,costo_promedio) VALUES($1,$2,$3,10,10)', [product, `${code}-ANNUL`, 'Material anulación']);
    const income = await http(inventoryUrl, '/kardex/documentos', 'POST', { tipo_movimiento: 'INGRESO', bodega_id: ids.source, referencia: `${code}-MANUAL`, detalles: [{ producto_id: product, cantidad: 5, costo_unitario: 10 }] });
    assert.equal(income.status, 201, JSON.stringify(income));
    await seedTransfer(product, ids.source, ids.dest, 4);
    const annul = await http(inventoryUrl, `/kardex/documentos/${income.data.id}/anular`, 'PATCH');
    assert.equal(annul.status, 400, JSON.stringify(annul));
    assert.match(JSON.stringify(annul.data), /pendiente de recepción/);
    const saved = (await db.query('SELECT stock_actual FROM kpi_inventory.tb_stock_bodega WHERE bodega_id=$1 AND producto_id=$2', [ids.source, product])).rows[0];
    assert.equal(Number(saved.stock_actual), 5);
  });
  await test('Kardex permite anular ingreso sin referencia aunque otra compra tenga referencia vacía', async () => {
    const product = randomUUID();
    const purchaseOrder = randomUUID();
    await db.query('INSERT INTO kpi_inventory.tb_orden_compra(id,codigo,referencia) VALUES($1,$2,NULL)', [purchaseOrder, `${code}-PO`]);
    await db.query('INSERT INTO kpi_inventory.tb_producto(id,codigo,nombre,ultimo_costo,costo_promedio) VALUES($1,$2,$3,10,10)', [product, `${code}-BLANK`, 'Material referencia vacía']);
    const income = await http(inventoryUrl, '/kardex/documentos', 'POST', { tipo_movimiento: 'INGRESO', bodega_id: ids.source, detalles: [{ producto_id: product, cantidad: 2, costo_unitario: 10 }] });
    assert.equal(income.status, 201, JSON.stringify(income));
    const annul = await http(inventoryUrl, `/kardex/documentos/${income.data.id}/anular`, 'PATCH');
    assert.equal(annul.status, 200, JSON.stringify(annul));
    assert.equal(annul.data.estado, 'ANULADO');
    const saved = (await db.query('SELECT stock_actual FROM kpi_inventory.tb_stock_bodega WHERE bodega_id=$1 AND producto_id=$2', [ids.source, product])).rows[0];
    assert.equal(Number(saved.stock_actual), 0);
  });
  await test('API de impresión captura una vez y conserva el reloj automático del equipo', async () => {
    ids.equipmentType = randomUUID(); ids.equipment = randomUUID();
    await db.query('INSERT INTO kpi_maintenance.tb_equipo_tipo(id,codigo,nombre) VALUES($1,$2,$3)', [ids.equipmentType, code, 'Tipo pruebas recepción']);
    await db.query("INSERT INTO kpi_maintenance.tb_equipo(id,codigo,nombre,equipo_tipo_id,horometro_actual,estado_funcionamiento) VALUES($1,$2,$3,$4,1000,'FUNCIONAMIENTO')", [ids.equipment, code, 'Equipo pruebas captura', ids.equipmentType]);
    await db.query("UPDATE kpi_maintenance.tb_equipo SET horometro_operativo_desde=(clock_timestamp() AT TIME ZONE 'America/Guayaquil')-interval '1 hour' WHERE id=$1", [ids.equipment]);
    await db.query("UPDATE kpi_process.tb_work_order SET equipment_id=$1,maintenance_kind='CEBADO',valor_json=$2::jsonb WHERE id=$3", [ids.equipment, JSON.stringify({ horometro_actual: null, horometro_anterior: 900 }), ids.wo]);
    const beforeEquipment = (await db.query('SELECT horometro_actual,estado_funcionamiento,horometro_operativo_desde FROM kpi_maintenance.tb_equipo WHERE id=$1', [ids.equipment])).rows[0];
    const beforeOrder = await http(maintenanceUrl, `/work-orders/${ids.wo}`);
    assert.equal(beforeOrder.status, 200, JSON.stringify(beforeOrder));
    assert.equal(beforeOrder.data.valor_json.horometro_actual, null);
    const started = await http(maintenanceUrl, `/work-orders/${ids.wo}/issue-documents/confirm`, 'POST');
    assert.equal(started.status, 201, JSON.stringify(started));
    const snapshot = (await db.query('SELECT status_workflow,valor_json FROM kpi_process.tb_work_order WHERE id=$1', [ids.wo])).rows[0];
    assert.equal(snapshot.status_workflow, 'IN_PROGRESS');
    assert.ok(Math.abs(Number(snapshot.valor_json.horometro_actual) - 1001) < 0.02, JSON.stringify(snapshot));
    assert.ok(snapshot.valor_json.horometro_capturado_en);
    assert.equal(snapshot.valor_json.horometro_anterior, 900);
    assert.deepEqual((await db.query('SELECT horometro_actual,estado_funcionamiento,horometro_operativo_desde FROM kpi_maintenance.tb_equipo WHERE id=$1', [ids.equipment])).rows[0], beforeEquipment);
    const reprinted = await http(maintenanceUrl, `/work-orders/${ids.wo}/issue-documents/confirm`, 'POST');
    assert.equal(reprinted.status, 201, JSON.stringify(reprinted));
    const reread = (await db.query('SELECT valor_json FROM kpi_process.tb_work_order WHERE id=$1', [ids.wo])).rows[0].valor_json;
    assert.deepEqual(reread, snapshot.valor_json);
    const live = (await db.query("SELECT horometro_actual+greatest(0,extract(epoch FROM ((clock_timestamp() AT TIME ZONE 'America/Guayaquil')-horometro_operativo_desde)))/3600 AS reading FROM kpi_maintenance.tb_equipo WHERE id=$1", [ids.equipment])).rows[0].reading;
    assert.ok(Number(live) >= Number(snapshot.valor_json.horometro_actual));
  });
  console.log(JSON.stringify({ passed: results.length, results, fixture: ids }));
}
main().catch(error => { results.push({ passed: false, error: error.stack }); console.error(error); process.exitCode = 1; }).finally(async () => {
  const output = resolve(__dirname, '../../outputs'); mkdirSync(output, { recursive: true });
  writeFileSync(resolve(output, '20261010-transfer-pending-integration.json'), JSON.stringify({ results, fixture: ids }, null, 2));
  await db.end();
});
