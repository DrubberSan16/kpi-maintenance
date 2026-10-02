import { KpiMaintenanceService } from './kpi-maintenance.service';

const service = Object.create(KpiMaintenanceService.prototype) as any;
describe('OT report dates in America/Guayaquil', () => {
  it('includes an evening OT in the Ecuador day even when its UTC date is the next day', () => {
    const range = service.buildSystemReportsDateRange({ from: '2026-10-01', to: '2026-10-01' });
    const row = { started_at: new Date('2026-10-02T00:50:09.476Z'), created_at: new Date('2026-10-02T00:48:20.410Z') };
    const reference = service.resolveWorkOrderReferenceDate(row);
    expect(reference.getTime()).toBeGreaterThanOrEqual(range.fromDate.getTime());
    expect(reference.getTime()).toBeLessThanOrEqual(range.toDate.getTime());
    expect(range.from).toBe('2026-10-01');
    expect(range.to).toBe('2026-10-01');
  });
  it('a planned order appears on its creation day and shows its future schedule separately', () => {
    const row = { created_at: new Date('2026-10-02T00:22:55Z'), scheduled_start: new Date('2026-10-03T00:00:00Z') };
    expect(service.currentGuayaquilDateString(service.resolveWorkOrderReferenceDate(row))).toBe('2026-10-01');
  });
  it('closed orders use the closure day, and date limits include 23:59 Ecuador time', () => {
    const range = service.buildSystemReportsDateRange({ from: '2026-10-01', to: '2026-10-01' });
    expect(range.fromDate.toISOString()).toBe('2026-10-01T05:00:00.000Z');
    expect(range.toDate.toISOString()).toBe('2026-10-02T04:59:59.999Z');
    const row = { closed_at: new Date('2026-10-03T00:15:00Z'), started_at: new Date('2026-10-02T00:15:00Z') };
    expect(service.currentGuayaquilDateString(service.resolveWorkOrderReferenceDate(row))).toBe('2026-10-02');
  });
});
