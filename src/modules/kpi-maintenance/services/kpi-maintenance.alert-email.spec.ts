import { KpiMaintenanceService } from './kpi-maintenance.service';

/**
 * Lo que ve quien recibe un correo de alerta.
 *
 * El correo mostraba la llave interna de la alerta —`WORK_ORDER:<uuid>:<evento>:
 * <epoch>`— y una fecha larga en español. Estas pruebas fijan lo contrario: una
 * referencia que se lee y una fecha `yyyy-MM-dd HH:mm`.
 */
type ServiceUnderTest = Record<string, any>;

function createService(): ServiceUnderTest {
  return Object.create(KpiMaintenanceService.prototype) as ServiceUnderTest;
}

describe('KpiMaintenanceService correos de alerta', () => {
  describe('formatAlertEmailDate', () => {
    it('devuelve yyyy-MM-dd HH:mm en hora de Guayaquil', () => {
      const service = createService();
      // 13:44 UTC son las 08:44 en Guayaquil (UTC-5).
      expect(service.formatAlertEmailDate('2026-09-16T13:44:00.000Z')).toBe(
        '2026-09-16 08:44',
      );
    });

    it('usa reloj de 24 horas, con medianoche en 00', () => {
      const service = createService();
      expect(service.formatAlertEmailDate('2026-09-16T05:00:00.000Z')).toBe(
        '2026-09-16 00:00',
      );
      expect(service.formatAlertEmailDate('2026-09-16T22:05:00.000Z')).toBe(
        '2026-09-16 17:05',
      );
    });

    it('acepta un Date y avisa cuando la fecha no sirve', () => {
      const service = createService();
      expect(
        service.formatAlertEmailDate(new Date('2026-01-02T15:30:00.000Z')),
      ).toBe('2026-01-02 10:30');
      expect(service.formatAlertEmailDate('cualquier cosa')).toBe(
        'No disponible',
      );
      expect(service.formatAlertEmailDate(null)).toBe('No disponible');
    });
  });

  describe('resolveAlertReferenceDisplay', () => {
    it('describe una OT por numero y titulo', () => {
      const service = createService();
      expect(
        service.resolveAlertReferenceDisplay(
          {
            referencia: 'WORK_ORDER:c5151c5a-9fe3-421d-ab42-40c029eeb6f2:FINALIZADA:1789566248988',
            referencia_tipo: 'WORK_ORDER',
          },
          {
            work_order_code: 'OT-A00180',
            work_order_title: 'Cambio de aceite UG02',
          },
        ),
      ).toBe('OT-A00180 - Cambio de aceite UG02');
    });

    it('nunca muestra la llave interna aunque falten codigo y titulo', () => {
      const service = createService();
      const shown = service.resolveAlertReferenceDisplay(
        {
          referencia: 'WORK_ORDER:c5151c5a-9fe3-421d-ab42-40c029eeb6f2:FINALIZADA:1789566248988',
          referencia_tipo: 'WORK_ORDER',
        },
        {},
      );
      expect(shown).toBe('Orden de trabajo');
      expect(shown).not.toContain('WORK_ORDER:');
      expect(shown).not.toContain('1789566248988');
    });

    it('se conforma con el numero cuando la OT no tiene titulo', () => {
      const service = createService();
      expect(
        service.resolveAlertReferenceDisplay(
          { referencia: 'WORK_ORDER:x', referencia_tipo: 'WORK_ORDER' },
          { work_order_code: 'OT-A00180' },
        ),
      ).toBe('OT-A00180');
    });

    it('deja intactas las referencias de otros modulos', () => {
      const service = createService();
      expect(
        service.resolveAlertReferenceDisplay(
          { referencia: 'PROGRAMACION:1', referencia_tipo: 'PROGRAMACION' },
          { plan_nombre: 'MPG 250 horas' },
        ),
      ).toBe('Programación · MPG 250 horas');
      expect(
        service.resolveAlertReferenceDisplay(
          { referencia: 'TANQUE-3', referencia_tipo: 'COMBUSTIBLE' },
          {},
        ),
      ).toBe('TANQUE-3');
    });
  });
});
