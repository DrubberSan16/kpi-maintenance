import {
  emailEnumLabel,
  emailHours,
  emailLabel,
  readableEmail,
} from './email-display';

const id = 'e9b6967d-1f3a-4baa-941e-092bdf52883c';

describe('identidad visible en correos', () => {
  it('conserva códigos de negocio y descarta IDs como etiquetas', () => {
    expect(emailLabel('Material sin registro', id, 'MAT-010 - Aceite')).toBe(
      'MAT-010 - Aceite',
    );
    expect(emailLabel('Bodega sin registro', '123', id)).toBe(
      'Bodega sin registro',
    );
    expect(emailLabel('Equipo no disponible', `Equipo ${id}`)).toBe(
      'Equipo no disponible',
    );
  });

  it('quita llaves técnicas del contenido visible y conserva enlaces y metadatos', () => {
    const mail = readableEmail({
      subject: `Alerta HOROMETRO:${id}:38855`,
      text: `Equipo ${id} · OT-A00163`,
      html: `<p>WORK_ORDER:${id}:FINALIZADA:1789566248988</p><a href="https://example.com/ot/${id}">Abrir OT-A00163</a>`,
      to: 'admin@example.com',
      headers: { 'X-Reference': id },
    });
    expect(mail.subject).toBe('Alerta Referencia no disponible');
    expect(mail.text).toBe('Equipo Referencia no disponible · OT-A00163');
    expect(mail.html).toContain(
      `<a href="https://example.com/ot/${id}">Abrir OT-A00163</a>`,
    );
    expect(mail.html).not.toContain('WORK_ORDER:');
    expect(mail.headers).toEqual({ 'X-Reference': id });
    expect(mail.to).toBe('admin@example.com');
  });

  it.each([
    ['SYSTEM', 'Sistema automático'],
    ['HOROMETRO_PROXIMO', 'Mantenimiento próximo por horómetro'],
    ['HOROMETRO_VENCIDO', 'Mantenimiento vencido por horómetro'],
    ['IN_PROGRESS', 'En proceso'],
    ['ORDEN_TRABAJO_BLOQUEADA', 'Orden de trabajo bloqueada'],
    ['NUEVO_EVENTO', 'Nuevo evento'],
  ])('describe %s en español', (value, label) => {
    expect(emailEnumLabel(value)).toBe(label);
  });

  it('muestra las horas sin inventar cero cuando falta la lectura', () => {
    expect(emailHours(null)).toBe('No disponible');
    expect(emailHours(undefined)).toBe('No disponible');
    expect(emailHours('')).toBe('No disponible');
    expect(emailHours('0')).toBe('0.00 h');
    expect(emailHours(38855)).toBe('38855.00 h');
  });
});
