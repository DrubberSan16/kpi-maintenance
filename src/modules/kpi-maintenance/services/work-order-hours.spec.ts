import { workOrderWorkedHours } from './work-order-hours';

const at = (hour: number) => `2026-10-16T${String(hour).padStart(2, '0')}:00:00-05:00`;
const event = (hour: number, from_status: string, to_status: string) => ({ changed_at: at(hour), from_status, to_status });

describe('Horas trabajadas de cebado', () => {
  const order = { started_at: at(8), closed_at: at(18), status_workflow: 'CLOSED' };
  it('descuenta todas las revisiones, también con historial descendente y eventos de edición', () => {
    const history = [event(8, 'PLANNED', 'IN_PROGRESS'), event(9, 'IN_PROGRESS', 'REVIEW'),
      event(10, 'REVIEW', 'REVIEW'), event(11, 'REVIEW', 'IN_PROGRESS'),
      event(13, 'IN_PROGRESS', 'REVIEW'), event(16, 'REVIEW', 'IN_PROGRESS'), event(18, 'IN_PROGRESS', 'CLOSED')];
    expect(workOrderWorkedHours(order, history.reverse(), new Date(at(20)).getTime())).toBe(5);
  });
  it('congela el tiempo durante una revisión vigente y al finalizar desde revisión', () => {
    const history = [event(10, 'IN_PROGRESS', 'REVIEW')];
    expect(workOrderWorkedHours({ ...order, closed_at: null, status_workflow: 'REVIEW' }, history, new Date(at(16)).getTime())).toBe(2);
    expect(workOrderWorkedHours(order, history, new Date(at(20)).getTime())).toBe(2);
  });
  it('usa la transición real si falta started_at y no inventa horas para una OT planificada', () => {
    expect(workOrderWorkedHours({}, [event(8, 'PLANNED', 'IN_PROGRESS'), event(10, 'IN_PROGRESS', 'CLOSED')], new Date(at(20)).getTime())).toBe(2);
    expect(workOrderWorkedHours({ status_workflow: 'PLANNED' }, [])).toBeNull();
  });
  it('interpreta timestamps sin zona como Guayaquil y conserva fracciones de hora', () => {
    expect(workOrderWorkedHours({ started_at: '2026-10-16T08:00:00', closed_at: '2026-10-16T08:30:00' }, [], new Date(at(20)).getTime())).toBe(0.5);
  });
  it('ignora started_at/closed_at pisados por guardar la cabecera después del cierre (OT-A00432)', () => {
    const rewritten = '2026-10-17T17:58:39.475';
    const order = {
      started_at: rewritten, closed_at: rewritten, status_workflow: 'CLOSED',
      valor_json: { execution_start: { at: '2026-10-17T21:17:54.226Z' } },
    };
    const history = [
      { changed_at: '2026-10-17T16:17:54.226', from_status: 'PLANNED', to_status: 'IN_PROGRESS' },
      { changed_at: '2026-10-17T17:58:06.520', from_status: 'IN_PROGRESS', to_status: 'CLOSED' },
      { changed_at: '2026-10-17T17:58:39.540', from_status: 'CLOSED', to_status: 'CLOSED' },
    ];
    const hours = workOrderWorkedHours(order, history, new Date('2026-10-18T00:00:00-05:00').getTime());
    expect(Math.round(hours! * 3600)).toBe(6012); // 01:40:12
    // Sin registro de inicio de ejecución, la transición a EN PROCESO basta.
    const legacy = workOrderWorkedHours({ ...order, valor_json: {} }, history, new Date('2026-10-18T00:00:00-05:00').getTime());
    expect(Math.round(legacy! * 3600)).toBe(6012);
  });
  it('acota eventos fuera de la ejecución y no cuenta después del cierre', () => {
    expect(workOrderWorkedHours(order, [event(6, 'IN_PROGRESS', 'REVIEW'), event(20, 'CLOSED', 'REVIEW')], new Date(at(22)).getTime())).toBe(10);
  });
});
