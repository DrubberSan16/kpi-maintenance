/**
 * Clasificación financiera del material consumido en una OT.
 *
 * Gerencia lee el costo de mantenimiento partido en repuestos, aceite e
 * insumos, y las tres categorías tienen que ser excluyentes: el aceite no puede
 * aparecer también como repuesto ni sumarse dos veces entre reportes.
 *
 * El catálogo no tiene un campo "repuesto / insumo", así que se decide con lo
 * que sí tiene cada material:
 *  - Aceite: marcado como aceite y medido en galones (la misma regla que el
 *    control de cebado y aceite, para que las dos cifras coincidan).
 *  - Repuestos: líneas de filtros, mecánico, eléctrico y equipos, o categoría
 *    repuesto. Es lo que se cambia en la máquina.
 *  - Insumos: el resto (consumibles inmediatos, limpieza, seguridad, general de
 *    estación). Se gasta al trabajar, no se instala.
 *
 * Mover un material de línea en el catálogo cambia su categoría aquí.
 */
export type MaterialCostCategory = 'REPUESTOS' | 'ACEITE' | 'INSUMOS';

/**
 * Aceite del control: material marcado como aceite y medido en galones.
 *
 * El reporte habla de galones. El filtro de aceite MTU (B7299) está marcado
 * como aceite pero se mide por unidad, y cada filtro entregado se sumaba como
 * si fuera un galón, con su costo dentro del costo de aceite.
 */
export function aceiteEnGalonesSql(producto: string) {
  return `COALESCE(${producto}.es_aceite, false) AND EXISTS (
    SELECT 1 FROM kpi_inventory.tb_unidad_medida um WHERE um.id = ${producto}.unidad_medida_id
      AND (UPPER(COALESCE(um.codigo, '')) LIKE 'GAL%' OR UPPER(COALESCE(um.nombre, '')) LIKE 'GAL%'))`;
}

const LINEAS_REPUESTO = [
  'FILTROS',
  'MECANICO',
  'MECÁNICO',
  'ELECTRICO',
  'ELÉCTRICO',
  'EQUIPOS Y TRANSFORMADORES',
];

/** Consulta la categoría de cada material; devuelve `id`, `categoria_costo`, `linea_nombre`. */
export const MATERIAL_COST_CATEGORY_SQL = `
  SELECT p.id::text AS id,
    CASE
      WHEN ${aceiteEnGalonesSql('p')} THEN 'ACEITE'
      WHEN UPPER(COALESCE(c.nombre, '')) LIKE 'REPUESTO%'
        OR UPPER(TRIM(COALESCE(l.nombre, ''))) IN (${LINEAS_REPUESTO.map((linea) => `'${linea}'`).join(', ')})
        THEN 'REPUESTOS'
      ELSE 'INSUMOS'
    END AS categoria_costo,
    l.nombre AS linea_nombre
  FROM kpi_inventory.tb_producto p
  LEFT JOIN kpi_inventory.tb_linea l ON l.id = p.linea_id
  LEFT JOIN kpi_inventory.tb_categoria c ON c.id = p.categoria_id
  WHERE p.id = ANY($1::uuid[])
`;
