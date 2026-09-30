import {
  armarCatalogoCargos,
  calcularValorHora,
  claveTexto,
  esCedulaConFormato,
  leerNumero,
  normalizarCedula,
  redondear,
  resolverCargo,
} from './empleado.utils';

describe('valor por hora', () => {
  it('es el sueldo entre 240 (30 días de 8 horas), con cuatro decimales', () => {
    expect(calcularValorHora(1200)).toBe(5);
    expect(calcularValorHora(700)).toBe(2.9167);
    expect(calcularValorHora(650)).toBe(2.7083);
    expect(calcularValorHora(470)).toBe(1.9583);
    expect(calcularValorHora(4000)).toBe(16.6667);
  });

  it('redondea el empate hacia arriba', () => {
    // 0,06 / 240 = 0,00025: el empate exacto sube a 0,0003.
    expect(calcularValorHora(0.06)).toBe(0.0003);
  });

  it('el redondeo comercial no cae por la representación binaria', () => {
    expect(redondear(1.005, 2)).toBe(1.01);
    expect(redondear(-1.005, 2)).toBe(-1.01);
    expect(redondear(2.675, 2)).toBe(2.68);
    expect(redondear(0, 2)).toBe(0);
  });
});

describe('cédula', () => {
  it('conserva solo los dígitos', () => {
    expect(normalizarCedula(2200243042)).toBe('2200243042');
    expect(normalizarCedula(' 0802531988 ')).toBe('0802531988');
    expect(normalizarCedula('080-253-1988')).toBe('0802531988');
  });

  it('devuelve el cero que Excel le quitó al número', () => {
    expect(normalizarCedula(604621326)).toBe('0604621326');
    expect(normalizarCedula('501512867')).toBe('0501512867');
  });

  it('descarta la tilde de más que traen algunas celdas', () => {
    // El Excel del personal tiene celdas escritas con una tilde (U+00B4)
    // delante del número.
    const conTilde = `${String.fromCharCode(0xb4)}0920403532`;
    expect(normalizarCedula(conTilde)).toBe('0920403532');
  });

  it('no inventa dígitos cuando faltan más de uno', () => {
    expect(normalizarCedula('12345')).toBe('12345');
    expect(normalizarCedula(null)).toBe('');
    expect(esCedulaConFormato('12345')).toBe(false);
    expect(esCedulaConFormato('0604621326')).toBe(true);
    expect(esCedulaConFormato('06046213260')).toBe(false);
  });
});

describe('texto', () => {
  it('la clave ignora mayúsculas, tildes y espacios', () => {
    expect(claveTexto('  Supervisor  de   SSA ')).toBe('SUPERVISOR DE SSA');
    expect(claveTexto('Técnico Mecánico')).toBe('TECNICO MECANICO');
    expect(claveTexto('CEDEÑO')).toBe('CEDENO');
    expect(claveTexto(null)).toBe('');
  });
});

describe('leerNumero', () => {
  it('lee números y textos con formatos de moneda', () => {
    expect(leerNumero(700)).toBe(700);
    expect(leerNumero('1200')).toBe(1200);
    expect(leerNumero('$ 700')).toBe(700);
    expect(leerNumero('1.200,50')).toBe(1200.5);
    expect(leerNumero('1,200.50')).toBe(1200.5);
    expect(leerNumero('700,5')).toBe(700.5);
    expect(leerNumero('700.5')).toBe(700.5);
  });

  it('un punto o una coma seguidos de tres dígitos son miles', () => {
    expect(leerNumero('1.200')).toBe(1200);
    expect(leerNumero('1,200')).toBe(1200);
    expect(leerNumero('1.200.000')).toBe(1200000);
  });

  it('devuelve null si no es un número', () => {
    expect(leerNumero('')).toBeNull();
    expect(leerNumero(null)).toBeNull();
    expect(leerNumero('abc')).toBeNull();
    expect(leerNumero(Number.NaN)).toBeNull();
  });
});

describe('cargos', () => {
  it('escoge la escritura más usada cuando un cargo está guardado de varias formas', () => {
    const catalogo = armarCatalogoCargos([
      { cargo: 'Bodeguero', total: 1 },
      { cargo: 'BODEGUERO', total: 4 },
      { cargo: 'Técnico Eléctrico', total: 2 },
    ]);
    expect(catalogo.get('BODEGUERO')).toBe('BODEGUERO');
    expect(catalogo.get('TECNICO ELECTRICO')).toBe('Técnico Eléctrico');
    expect(catalogo.size).toBe(2);
  });

  it('reutiliza la escritura existente aunque cambien mayúsculas, tildes o espacios', () => {
    const catalogo = armarCatalogoCargos([
      { cargo: 'SUPERVISOR DE SSA', total: 3 },
    ]);
    expect(resolverCargo('supervisor  de ssa ', catalogo)).toBe(
      'SUPERVISOR DE SSA',
    );
    expect(resolverCargo('Supervisor de SSÁ', catalogo)).toBe(
      'SUPERVISOR DE SSA',
    );
  });

  it('un cargo nuevo se anota para que el siguiente igual lo reutilice', () => {
    const catalogo = armarCatalogoCargos([]);
    expect(resolverCargo('Jefe de Campo', catalogo)).toBe('Jefe de Campo');
    expect(resolverCargo('JEFE DE CAMPO', catalogo)).toBe('Jefe de Campo');
    expect(catalogo.size).toBe(1);
  });

  it('un cargo vacío no se registra', () => {
    const catalogo = armarCatalogoCargos([]);
    expect(resolverCargo('   ', catalogo)).toBe('');
    expect(catalogo.size).toBe(0);
  });
});
