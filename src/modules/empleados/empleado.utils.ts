/**
 * Reglas de los empleados que no dependen de la base de datos: el valor por
 * hora, la cédula y los cargos.
 */

/**
 * Horas de la jornada mensual ordinaria en Ecuador: 30 días de 8 horas.
 *
 * Es la base con la que el Código del Trabajo calcula el valor de la hora
 * (y, sobre ella, los recargos de las suplementarias y extraordinarias).
 */
export const HORAS_MES_ECUADOR = 240;

/**
 * Decimales con los que se guarda el valor por hora.
 *
 * Un costo unitario que nace de una división no se redondea en el origen:
 * 700 / 240 = 2,9166… se guarda como 2,9167 y la pantalla lo presenta con dos
 * decimales. Redondearlo a centavos al guardar perdería precisión en cada
 * suma de horas.
 */
export const DECIMALES_VALOR_HORA = 4;

export const DECIMALES_SUELDO = 2;

/**
 * Texto de un dato suelto (una celda de Excel, un campo de un JSON): solo
 * lo que es texto, número o booleano. Un objeto no se convierte en
 * "[object Object]", se toma como vacío.
 */
export function aTexto(value: unknown): string {
  if (typeof value === 'string') return value;
  if (
    typeof value === 'number' ||
    typeof value === 'bigint' ||
    typeof value === 'boolean'
  ) {
    return String(value);
  }
  return '';
}

/** Quita espacios repetidos y de los extremos. */
export function normalizarEspacios(value: unknown): string {
  return aTexto(value).replace(/\s+/g, ' ').trim();
}

/**
 * Clave para comparar dos textos sin importar mayúsculas, tildes ni espacios:
 * "Supervisor de SSA " y "SUPERVISOR DE SSÁ" dan la misma clave.
 */
export function claveTexto(value: unknown): string {
  return normalizarEspacios(value)
    .normalize('NFD')
    .replace(/\p{M}/gu, '')
    .toUpperCase();
}

/**
 * Cédula lista para guardar: solo dígitos.
 *
 * Excel guarda la cédula como un número y le quita el cero inicial
 * (0604621326 llega como 604621326), y hay celdas escritas con una tilde de
 * más delante del número. Por eso se descarta todo lo que no sea un dígito y,
 * si queda de nueve, se le devuelve el cero. No valida nada más: quien la
 * llama comprueba que sean diez.
 */
export function normalizarCedula(value: unknown): string {
  const digitos = aTexto(value).replace(/\D/g, '');
  return digitos.length === 9 ? `0${digitos}` : digitos;
}

export function esCedulaConFormato(value: string): boolean {
  return /^\d{10}$/.test(value);
}

/**
 * Redondeo comercial a `decimales` decimales: el empate se aleja del cero.
 * `toPrecision(15)` deshace el error de la representación binaria antes de
 * redondear, porque sin él 1,005 * 100 es 100,49999999999999 y bajaría.
 */
export function redondear(value: number, decimales: number): number {
  const factor = 10 ** decimales;
  const escalado = Number((Math.abs(value) * factor).toPrecision(15));
  const redondeado = Math.round(escalado) / factor;
  return value < 0 && redondeado !== 0 ? -redondeado : redondeado;
}

/**
 * Valor de la hora ordinaria: sueldo mensual / 240 (30 días x 8 horas),
 * con cuatro decimales.
 */
export function calcularValorHora(sueldo: number): number {
  return redondear(sueldo / HORAS_MES_ECUADOR, DECIMALES_VALOR_HORA);
}

/**
 * Convierte lo que llegue en una celda o en un JSON a número.
 * Acepta "1200", "1.200", "1.200,50", "1,200.50", "700,5" y "$ 700"; devuelve
 * `null` si no es un número.
 */
export function leerNumero(value: unknown): number | null {
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  const texto = aTexto(value).replace(/[^\d.,-]/g, '');
  if (!texto) return null;
  let limpio = texto;
  if (/^-?\d{1,3}([.,]\d{3})+$/.test(texto)) {
    // "1.200" y "1,200" son miles, no decimales: un sueldo no lleva tres.
    limpio = texto.replace(/[.,]/g, '');
  } else if (texto.includes(',') && texto.includes('.')) {
    // El separador que aparece último es el decimal; el otro agrupa miles.
    limpio =
      texto.lastIndexOf(',') > texto.lastIndexOf('.')
        ? texto.replace(/\./g, '').replace(',', '.')
        : texto.replace(/,/g, '');
  } else if (texto.includes(',')) {
    limpio = texto.replace(',', '.');
  }
  const numero = Number(limpio);
  return Number.isFinite(numero) ? numero : null;
}

/**
 * Cargos ya registrados: clave de comparación -> escritura con la que se
 * guardó. Una misma clave puede haberse guardado de varias formas; gana la
 * más usada y, si empatan, la primera por orden alfabético.
 */
export type CatalogoCargos = Map<string, string>;

export function armarCatalogoCargos(
  filas: Array<{ cargo: string; total: number }>,
): CatalogoCargos {
  const porClave = new Map<string, Array<{ cargo: string; total: number }>>();
  for (const fila of filas) {
    const cargo = normalizarEspacios(fila.cargo);
    const clave = claveTexto(cargo);
    if (!clave) continue;
    const grupo = porClave.get(clave) ?? [];
    grupo.push({ cargo, total: Number(fila.total) || 0 });
    porClave.set(clave, grupo);
  }
  const catalogo: CatalogoCargos = new Map();
  for (const [clave, grupo] of porClave) {
    grupo.sort(
      (a, b) => b.total - a.total || a.cargo.localeCompare(b.cargo, 'es'),
    );
    catalogo.set(clave, grupo[0].cargo);
  }
  return catalogo;
}

/**
 * Cargo tal como debe guardarse: si ya existe uno igual salvo mayúsculas,
 * tildes o espacios, se reutiliza su escritura; si es nuevo, se anota en el
 * catálogo para que el siguiente igual lo reutilice (una importación trae el
 * mismo cargo escrito de varias formas).
 */
export function resolverCargo(
  texto: unknown,
  catalogo: CatalogoCargos,
): string {
  const limpio = normalizarEspacios(texto);
  if (!limpio) return '';
  const clave = claveTexto(limpio);
  const existente = catalogo.get(clave);
  if (existente) return existente;
  catalogo.set(clave, limpio);
  return limpio;
}
