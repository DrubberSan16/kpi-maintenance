import {
  BadRequestException,
  ConflictException,
  Inject,
  Injectable,
  Logger,
  NotFoundException,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { createHash, randomUUID } from 'crypto';
import { URLSearchParams } from 'url';
import {
  appendFile,
  mkdir,
  readFile,
  readdir,
  rm,
  unlink,
  writeFile,
} from 'fs/promises';
import { basename, extname, join } from 'path';
import * as XLSX from 'xlsx';
import { InjectDataSource, InjectRepository } from '@nestjs/typeorm';
import {
  DataSource,
  FindOptionsWhere,
  In,
  IsNull,
  ObjectLiteral,
  Repository,
} from 'typeorm';
import {
  AlertaMantenimientoEntity,
  AnalisisLubricanteDetalleEntity,
  AnalisisLubricanteEntity,
  BitacoraDiariaEntity,
  ConsumoRepuestoEntity,
  ControlComponenteEntity,
  CronogramaSemanalDetalleEntity,
  CronogramaSemanalEntity,
  EntregaMaterialDetEntity,
  EntregaMaterialEntity,
  EquipoComponenteEntity,
  EquipoEntity,
  EquipoTipoEntity,
  EstadoEquipoCatalogoEntity,
  EstadoEquipoEntity,
  EventoProcesoEntity,
  EventoEquipoEntity,
  FallaCatalogoEntity,
  KardexEntity,
  LecturaEquipoEntity,
  LocationEntity,
  MarcaEntity,
  LubricacionPuntoEntity,
  MovimientoInventarioDetEntity,
  MovimientoInventarioEntity,
  PlanMantenimientoEntity,
  PlanTareaEntity,
  ProductoEntity,
  ProcedimientoActividadEntity,
  ProcedimientoPlantillaEntity,
  BodegaEntity,
  ProgramacionPlanEntity,
  ReservaStockEntity,
  ReporteCombustibleEntity,
  ReporteOperacionDiariaEntity,
  ReporteOperacionDiariaUnidadEntity,
  WorkOrderStatusHistoryEntity,
  StockBodegaEntity,
  WorkOrderAdjuntoEntity,
  WorkOrderEntity,
  WorkOrderTareaEntity,
} from '../entities/kpi-maintenance.entity';
import {
  AlertaQueryDto,
  AnalisisLubricanteCatalogQueryDto,
  AnalisisLubricanteDashboardQueryDto,
  CreateAnalisisLubricanteDto,
  ChangeEstadoDto,
  ComponenteQueryDto,
  CreateBitacoraDto,
  CreateComponenteDto,
  CreateConsumoDto,
  CreateCronogramaSemanalDto,
  CreateEquipoDto,
  CreateEquipoTipoDto,
  CreateEventoDto,
  CreateFallaCatalogoDto,
  CreateLecturaEquipoDto,
  CreateLubricacionPuntoDto,
  CreateLocationDto,
  CreatePlanDto,
  CreatePlanTareaDto,
  CreateProcedimientoPlantillaDto,
  CreateProgramacionDto,
  CreateReporteOperacionDiariaDto,
  CreateWorkOrderDto,
  CreateWorkOrderTareaDto,
  DateRangeDto,
  EquipoQueryDto,
  EquipoTipoQueryDto,
  EventoProcesoQueryDto,
  ImportAnalisisLubricanteBatchDto,
  IssueMaterialsDto,
  PurgeAnalisisLubricanteDto,
  UpdateAnalisisLubricanteDto,
  LocationQueryDto,
  UpdateBitacoraDto,
  UpdateComponenteDto,
  UpdateCronogramaSemanalDto,
  UpdateEquipoDto,
  UpdateEquipoTipoDto,
  UpdateFallaCatalogoDto,
  UpdateLecturaEquipoDto,
  UpdateLocationDto,
  UpdateLubricacionPuntoDto,
  UpdatePlanDto,
  UpdatePlanTareaDto,
  UpdateProcedimientoPlantillaDto,
  UpdateProgramacionDto,
  UpdateReporteOperacionDiariaDto,
  UpdateWorkOrderDto,
  UpdateWorkOrderTareaDto,
  UploadWorkOrderAdjuntoDto,
  WorkOrderAdjuntoQueryDto,
  WorkOrderQueryDto,
} from '../dto';

type AlertLevel = 'INFO' | 'WARNING' | 'CRITICAL';
type AlertCategory =
  | 'MANTENIMIENTO'
  | 'OPERACION'
  | 'LUBRICANTE'
  | 'COMBUSTIBLE'
  | 'INVENTARIO'
  | 'DATOS';
type AlertOrigin =
  | 'SYSTEM'
  | 'PROGRAMACION'
  | 'REPORTE_DIARIO'
  | 'ANALISIS_LUBRICANTE'
  | 'COMBUSTIBLE'
  | 'INVENTARIO'
  | 'BITACORA';
type CodeResolution = {
  requestedCode: string | null;
  resolvedCode: string;
  codeWasReassigned: boolean;
  reassignmentReason: string | null;
};

type LubricantMetricGroupKey =
  | 'ESTADO_LUBRICANTE'
  | 'DEGRADACION_QUIMICA'
  | 'CONTAMINACION'
  | 'DESGASTE'
  | 'OTROS_ELEMENTOS'
  | 'ADITIVOS';

type LubricantMetricDefinition = {
  key: string;
  label: string;
  group: LubricantMetricGroupKey;
  order: number;
  unit?: string;
  chartGroup: 'estado' | 'desgaste' | 'contaminacion' | 'otros';
  aliases?: string[];
  lowerWarnMultiplier?: number;
  upperWarnMultiplier?: number;
  lowerAlertMultiplier?: number;
  upperAlertMultiplier?: number;
  numericWarningMin?: number;
  numericAlertMin?: number;
  textAlert?: string[];
  textWarning?: string[];
};

type AlertCandidate = {
  equipo_id?: string | null;
  tipo_alerta: string;
  categoria: AlertCategory;
  nivel: AlertLevel;
  origen: AlertOrigin;
  referencia_tipo?: string | null;
  referencia?: string | null;
  detalle: string;
  payload_json: Record<string, unknown>;
};

type AnalisisLubricanteSaveOptions = {
  skipAlertRecalc?: boolean;
  skipProcessEvent?: boolean;
};

type LubricantImportLogLevel = 'INFO' | 'WARNING' | 'ERROR';
type LubricantImportJobStatus =
  | 'QUEUED'
  | 'PARSING'
  | 'PROCESSING'
  | 'COMPLETED'
  | 'FAILED';

type LubricantImportLogEntry = {
  at: string;
  level: LubricantImportLogLevel;
  message: string;
  context?: Record<string, unknown> | null;
};

type LubricantImportJobState = {
  id: string;
  status: LubricantImportJobStatus;
  progress: number;
  current_step: string;
  current_index: number;
  total_steps: number;
  upsert_existing: boolean;
  requested_by: string | null;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  source_file_name: string;
  stored_file_name: string;
  stored_file_path: string;
  file_size_bytes: number;
  warnings: string[];
  errors: Array<{ index: number; message: string }>;
  logs: LubricantImportLogEntry[];
  summary: {
    total: number;
    created: number;
    updated: number;
    skipped: number;
    imported_ids: string[];
  };
  error_message: string | null;
};

type ParsedLubricantWorkbook = {
  analyses: CreateAnalisisLubricanteDto[];
  warnings: string[];
  sheets: string[];
};

const LUBRICANT_GROUP_LABELS: Record<LubricantMetricGroupKey, string> = {
  ESTADO_LUBRICANTE: 'Estado del lubricante',
  DEGRADACION_QUIMICA: 'Degradación química',
  CONTAMINACION: 'Contaminación del lubricante',
  DESGASTE: 'Desgaste del equipo',
  OTROS_ELEMENTOS: 'Otros elementos',
  ADITIVOS: 'Presencia de aditivos',
};

const LUBRICANT_IMPORT_SHEETS = [
  'MOTOR',
  'HIDRAULICO',
  'TRANSMISION',
  'INDUSTRTIAL',
  'DIFERENCIAL DELANTERO',
  'DIFERENCIAL POSTERIOR',
  'SWING',
  'ENGRANAJE DE LA BOMBA',
  'MANDO FINAL DERECHO',
  'MANDO FINAL IZQUIERDO',
  'TANDEM DERECHO',
  'TANDEM IZQUIERDO',
];

const LUBRICANT_IMPORT_PARAMETER_ROWS = [
  { row: 22, label: 'Viscosidad a 100ÂºC, cSt' },
  { row: 23, label: 'Viscosidad a 40ÂºC, cSt' },
  { row: 24, label: 'Indice de Viscosidad' },
  { row: 25, label: 'T.B.N. mgKOH/gr' },
  { row: 26, label: 'Humedad' },
  { row: 27, label: 'Glycol, Abs/cm' },
  { row: 28, label: 'Combustible' },
  { row: 32, label: 'OxidaciÃ³n, Abs/cm' },
  { row: 33, label: 'NitraciÃ³n, Abs/cm' },
  { row: 34, label: 'SulfataciÃ³n, Abs/cm' },
  { row: 35, label: 'HollÃ­n, wt%' },
  { row: 39, label: 'Si (Silicio)' },
  { row: 40, label: 'Na (Sodio)' },
  { row: 41, label: 'Vanadio (V)' },
  { row: 42, label: 'Ni (Niquel)' },
  { row: 46, label: 'Fe (Hierro)' },
  { row: 47, label: 'Cr (Cromo)' },
  { row: 48, label: 'Al (Aluminio)' },
  { row: 49, label: 'Cu (Cobre)' },
  { row: 50, label: 'Pb (Plomo)' },
  { row: 51, label: 'EstaÃ±o (Sn)' },
  { row: 55, label: 'Mo (Molibdeno)' },
  { row: 56, label: 'B (Boro)' },
  { row: 57, label: 'Ba (Bario)' },
  { row: 58, label: 'Ti (Titanio)' },
  { row: 59, label: 'Ag (Plata)' },
  { row: 63, label: 'Ca (Calcio)' },
  { row: 64, label: 'Mg (Magnesio)' },
  { row: 65, label: 'Zn (Zinc)' },
  { row: 66, label: 'P (Fosforo)' },
] as const;

const LUBRICANT_METRIC_DEFINITIONS: LubricantMetricDefinition[] = [
  {
    key: 'VISCOSIDAD_100C',
    label: 'Viscosidad a 100ºC, cSt',
    group: 'ESTADO_LUBRICANTE',
    order: 1,
    unit: 'cSt',
    chartGroup: 'estado',
    aliases: ['viscosidad a 100c', 'viscosidad a 100 c', 'viscosidad a 100°c', 'viscosidad a 100ºc'],
    lowerWarnMultiplier: 0.9,
    upperWarnMultiplier: 1.1,
    lowerAlertMultiplier: 0.85,
    upperAlertMultiplier: 1.15,
  },
  {
    key: 'VISCOSIDAD_40C',
    label: 'Viscosidad a 40ºC, cSt',
    group: 'ESTADO_LUBRICANTE',
    order: 2,
    unit: 'cSt',
    chartGroup: 'estado',
    aliases: ['viscosidad a 40c', 'viscosidad a 40 c', 'viscosidad a 40°c', 'viscosidad a 40ºc'],
    lowerWarnMultiplier: 0.9,
    upperWarnMultiplier: 1.1,
    lowerAlertMultiplier: 0.85,
    upperAlertMultiplier: 1.15,
  },
  {
    key: 'INDICE_VISCOSIDAD',
    label: 'Indice de Viscosidad',
    group: 'ESTADO_LUBRICANTE',
    order: 3,
    chartGroup: 'estado',
    aliases: ['indice de viscosidad', 'índice de viscosidad'],
  },
  {
    key: 'TBN',
    label: 'T.B.N. mgKOH/gr',
    group: 'ESTADO_LUBRICANTE',
    order: 4,
    unit: 'mgKOH/gr',
    chartGroup: 'estado',
    aliases: ['tbn', 't.b.n. mgkoh/gr', 'tbn mgkoh/gr'],
    lowerWarnMultiplier: 0.5,
    lowerAlertMultiplier: 0.35,
  },
  {
    key: 'HUMEDAD',
    label: 'Humedad',
    group: 'ESTADO_LUBRICANTE',
    order: 5,
    chartGroup: 'estado',
    textAlert: ['positivo', 'presente'],
  },
  {
    key: 'GLYCOL',
    label: 'Glycol, Abs/cm',
    group: 'ESTADO_LUBRICANTE',
    order: 6,
    unit: 'Abs/cm',
    chartGroup: 'estado',
    aliases: ['glycol', 'glycol abs/cm'],
    numericAlertMin: 2,
  },
  {
    key: 'COMBUSTIBLE',
    label: 'Combustible',
    group: 'ESTADO_LUBRICANTE',
    order: 7,
    chartGroup: 'estado',
    textAlert: ['positivo', 'presente'],
  },
  {
    key: 'OXIDACION',
    label: 'Oxidación, Abs/cm',
    group: 'DEGRADACION_QUIMICA',
    order: 1,
    unit: 'Abs/cm',
    chartGroup: 'estado',
    aliases: ['oxidacion', 'oxidación'],
    numericWarningMin: 20,
    numericAlertMin: 30,
  },
  {
    key: 'NITRACION',
    label: 'Nitración, Abs/cm',
    group: 'DEGRADACION_QUIMICA',
    order: 2,
    unit: 'Abs/cm',
    chartGroup: 'estado',
    aliases: ['nitracion', 'nitración'],
    numericWarningMin: 10,
    numericAlertMin: 15,
  },
  {
    key: 'SULFATACION',
    label: 'Sulfatación, Abs/cm',
    group: 'DEGRADACION_QUIMICA',
    order: 3,
    unit: 'Abs/cm',
    chartGroup: 'estado',
    aliases: ['sulfatacion', 'sulfatación'],
    numericWarningMin: 20,
    numericAlertMin: 30,
  },
  {
    key: 'HOLLIN',
    label: 'Hollín, wt%',
    group: 'DEGRADACION_QUIMICA',
    order: 4,
    unit: 'wt%',
    chartGroup: 'estado',
    aliases: ['hollin', 'hollín', 'hollin wt%', 'hollín wt%'],
    numericWarningMin: 0.75,
    numericAlertMin: 1.2,
  },
  {
    key: 'SI',
    label: 'Si (Silicio)',
    group: 'CONTAMINACION',
    order: 1,
    unit: 'ppm',
    chartGroup: 'contaminacion',
    aliases: ['si', 'si silicio', 'silicio'],
  },
  {
    key: 'NA',
    label: 'Na (Sodio)',
    group: 'CONTAMINACION',
    order: 2,
    unit: 'ppm',
    chartGroup: 'contaminacion',
    aliases: ['na', 'na sodio', 'sodio'],
  },
  {
    key: 'V',
    label: 'Vanadio (V)',
    group: 'CONTAMINACION',
    order: 3,
    unit: 'ppm',
    chartGroup: 'contaminacion',
    aliases: ['v', 'vanadio'],
  },
  {
    key: 'NI',
    label: 'Ni (Niquel)',
    group: 'CONTAMINACION',
    order: 4,
    unit: 'ppm',
    chartGroup: 'contaminacion',
    aliases: ['ni', 'niquel', 'níquel'],
  },
  {
    key: 'FE',
    label: 'Fe (Hierro)',
    group: 'DESGASTE',
    order: 1,
    unit: 'ppm',
    chartGroup: 'desgaste',
    aliases: ['fe', 'hierro'],
  },
  {
    key: 'CR',
    label: 'Cr (Cromo)',
    group: 'DESGASTE',
    order: 2,
    unit: 'ppm',
    chartGroup: 'desgaste',
    aliases: ['cr', 'cromo'],
  },
  {
    key: 'AL',
    label: 'Al (Aluminio)',
    group: 'DESGASTE',
    order: 3,
    unit: 'ppm',
    chartGroup: 'desgaste',
    aliases: ['al', 'aluminio'],
  },
  {
    key: 'CU',
    label: 'Cu (Cobre)',
    group: 'DESGASTE',
    order: 4,
    unit: 'ppm',
    chartGroup: 'desgaste',
    aliases: ['cu', 'cobre'],
  },
  {
    key: 'PB',
    label: 'Pb (Plomo)',
    group: 'DESGASTE',
    order: 5,
    unit: 'ppm',
    chartGroup: 'desgaste',
    aliases: ['pb', 'plomo'],
  },
  {
    key: 'SN',
    label: 'Estaño (Sn)',
    group: 'DESGASTE',
    order: 6,
    unit: 'ppm',
    chartGroup: 'desgaste',
    aliases: ['sn', 'estaño', 'estano'],
  },
  {
    key: 'MO',
    label: 'Mo (Molibdeno)',
    group: 'OTROS_ELEMENTOS',
    order: 1,
    unit: 'ppm',
    chartGroup: 'otros',
    aliases: ['mo', 'molibdeno'],
  },
  {
    key: 'B',
    label: 'B (Boro)',
    group: 'OTROS_ELEMENTOS',
    order: 2,
    unit: 'ppm',
    chartGroup: 'otros',
    aliases: ['b', 'boro'],
  },
  {
    key: 'BA',
    label: 'Ba (Bario)',
    group: 'OTROS_ELEMENTOS',
    order: 3,
    unit: 'ppm',
    chartGroup: 'otros',
    aliases: ['ba', 'bario'],
  },
  {
    key: 'TI',
    label: 'Ti (Titanio)',
    group: 'OTROS_ELEMENTOS',
    order: 4,
    unit: 'ppm',
    chartGroup: 'otros',
    aliases: ['ti', 'titanio'],
  },
  {
    key: 'AG',
    label: 'Ag (Plata)',
    group: 'OTROS_ELEMENTOS',
    order: 5,
    unit: 'ppm',
    chartGroup: 'otros',
    aliases: ['ag', 'plata'],
  },
  {
    key: 'CA',
    label: 'Ca (Calcio)',
    group: 'ADITIVOS',
    order: 1,
    unit: 'ppm',
    chartGroup: 'otros',
    aliases: ['ca', 'calcio'],
  },
  {
    key: 'MG',
    label: 'Mg (Magnesio)',
    group: 'ADITIVOS',
    order: 2,
    unit: 'ppm',
    chartGroup: 'otros',
    aliases: ['mg', 'magnesio'],
  },
  {
    key: 'ZN',
    label: 'Zn (Zinc)',
    group: 'ADITIVOS',
    order: 3,
    unit: 'ppm',
    chartGroup: 'otros',
    aliases: ['zn', 'zinc'],
  },
  {
    key: 'P',
    label: 'P (Fosforo)',
    group: 'ADITIVOS',
    order: 4,
    unit: 'ppm',
    chartGroup: 'otros',
    aliases: ['p', 'fosforo', 'fósforo'],
  },
];

@Injectable()
export class KpiMaintenanceService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KpiMaintenanceService.name);
  private recalculationInterval: NodeJS.Timeout | null = null;
  private recalculationRunning = false;

  private readonly RECALCULATION_INTERVAL_MS = 60 * 1000;
  private readonly RECALCULATION_BATCH_SIZE = 100;
  private readonly RECALCULATION_WORKERS = 4;
  private readonly MAX_STORED_ERRORS = 200;

  constructor(
    @InjectRepository(EquipoEntity)
    private readonly equipoRepo: Repository<EquipoEntity>,
    @InjectRepository(EquipoTipoEntity)
    private readonly equipoTipoRepo: Repository<EquipoTipoEntity>,
    @InjectRepository(EquipoComponenteEntity)
    private readonly equipoComponenteRepo: Repository<EquipoComponenteEntity>,
    @InjectRepository(LocationEntity)
    private readonly locationRepo: Repository<LocationEntity>,
    @InjectRepository(MarcaEntity)
    private readonly marcaRepo: Repository<MarcaEntity>,
    @InjectRepository(BitacoraDiariaEntity)
    private readonly bitacoraRepo: Repository<BitacoraDiariaEntity>,
    @InjectRepository(AlertaMantenimientoEntity)
    private readonly alertaRepo: Repository<AlertaMantenimientoEntity>,
    @InjectRepository(EstadoEquipoEntity)
    private readonly estadoRepo: Repository<EstadoEquipoEntity>,
    @InjectRepository(EstadoEquipoCatalogoEntity)
    private readonly estadoCatalogoRepo: Repository<EstadoEquipoCatalogoEntity>,
    @InjectRepository(EventoEquipoEntity)
    private readonly eventoRepo: Repository<EventoEquipoEntity>,
    @InjectRepository(FallaCatalogoEntity)
    private readonly fallaRepo: Repository<FallaCatalogoEntity>,
    @InjectRepository(LecturaEquipoEntity)
    private readonly lecturaRepo: Repository<LecturaEquipoEntity>,
    @InjectRepository(LubricacionPuntoEntity)
    private readonly lubricacionRepo: Repository<LubricacionPuntoEntity>,
    @InjectRepository(ProcedimientoPlantillaEntity)
    private readonly procedimientoRepo: Repository<ProcedimientoPlantillaEntity>,
    @InjectRepository(ProcedimientoActividadEntity)
    private readonly procedimientoActividadRepo: Repository<ProcedimientoActividadEntity>,
    @InjectRepository(AnalisisLubricanteEntity)
    private readonly analisisLubricanteRepo: Repository<AnalisisLubricanteEntity>,
    @InjectRepository(AnalisisLubricanteDetalleEntity)
    private readonly analisisLubricanteDetRepo: Repository<AnalisisLubricanteDetalleEntity>,
    @InjectRepository(CronogramaSemanalEntity)
    private readonly cronogramaSemanalRepo: Repository<CronogramaSemanalEntity>,
    @InjectRepository(CronogramaSemanalDetalleEntity)
    private readonly cronogramaSemanalDetRepo: Repository<CronogramaSemanalDetalleEntity>,
    @InjectRepository(ReporteOperacionDiariaEntity)
    private readonly reporteDiarioRepo: Repository<ReporteOperacionDiariaEntity>,
    @InjectRepository(ReporteOperacionDiariaUnidadEntity)
    private readonly reporteDiarioUnidadRepo: Repository<ReporteOperacionDiariaUnidadEntity>,
    @InjectRepository(ReporteCombustibleEntity)
    private readonly reporteCombustibleRepo: Repository<ReporteCombustibleEntity>,
    @InjectRepository(ControlComponenteEntity)
    private readonly controlComponenteRepo: Repository<ControlComponenteEntity>,
    @InjectRepository(EventoProcesoEntity)
    private readonly eventoProcesoRepo: Repository<EventoProcesoEntity>,
    @InjectRepository(PlanMantenimientoEntity)
    private readonly planRepo: Repository<PlanMantenimientoEntity>,
    @InjectRepository(PlanTareaEntity)
    private readonly planTareaRepo: Repository<PlanTareaEntity>,
    @InjectRepository(ProgramacionPlanEntity)
    private readonly programacionRepo: Repository<ProgramacionPlanEntity>,
    @InjectRepository(WorkOrderEntity)
    private readonly woRepo: Repository<WorkOrderEntity>,
    @InjectRepository(WorkOrderStatusHistoryEntity)
    private readonly woHistoryRepo: Repository<WorkOrderStatusHistoryEntity>,
    @InjectRepository(ConsumoRepuestoEntity)
    private readonly consumoRepo: Repository<ConsumoRepuestoEntity>,
    @InjectRepository(StockBodegaEntity)
    private readonly stockRepo: Repository<StockBodegaEntity>,
    @InjectRepository(KardexEntity)
    private readonly kardexRepo: Repository<KardexEntity>,
    @InjectRepository(ProductoEntity)
    private readonly productoRepo: Repository<ProductoEntity>,
    @InjectRepository(BodegaEntity)
    private readonly bodegaRepo: Repository<BodegaEntity>,
    @InjectRepository(ReservaStockEntity)
    private readonly reservaRepo: Repository<ReservaStockEntity>,
    @InjectRepository(WorkOrderTareaEntity)
    private readonly woTareaRepo: Repository<WorkOrderTareaEntity>,
    @InjectRepository(WorkOrderAdjuntoEntity)
    private readonly woAdjuntoRepo: Repository<WorkOrderAdjuntoEntity>,
    @InjectDataSource() private readonly dataSource: DataSource,
  ) {}

  private readonly uploadRoot =
    process.env.WORK_ORDER_UPLOAD_DIR ||
    join(process.cwd(), 'storage', 'work-orders');
  private readonly lubricantImportRoot =
    process.env.LUBRICANT_IMPORT_DIR ||
    join(process.cwd(), 'storage', 'lubricant-imports');
  private readonly lubricantImportJobs = new Map<string, LubricantImportJobState>();

  private buildProductoLabel(producto?: Partial<ProductoEntity> | null) {
    if (!producto) return null;
    const codigo = String(producto.codigo || '').trim();
    const nombre = String(producto.nombre || '').trim();
    if (codigo && nombre) return `${codigo} - ${nombre}`;
    return nombre || codigo || null;
  }

  private buildBodegaLabel(bodega?: Partial<BodegaEntity> | null) {
    if (!bodega) return null;
    const codigo = String(bodega.codigo || '').trim();
    const nombre = String(bodega.nombre || '').trim();
    if (codigo && nombre) return `${codigo} - ${nombre}`;
    return nombre || codigo || null;
  }

  private async buildInventoryCatalogMaps(productIds: string[], warehouseIds: string[]) {
    const uniqueProductIds = [...new Set(productIds.filter(Boolean))];
    const uniqueWarehouseIds = [...new Set(warehouseIds.filter(Boolean))];

    const [productos, bodegas] = await Promise.all([
      uniqueProductIds.length
        ? this.productoRepo.find({ where: { id: In(uniqueProductIds) } })
        : Promise.resolve([] as ProductoEntity[]),
      uniqueWarehouseIds.length
        ? this.bodegaRepo.find({ where: { id: In(uniqueWarehouseIds) } })
        : Promise.resolve([] as BodegaEntity[]),
    ]);

    return {
      productMap: new Map(productos.map((item) => [item.id, item])),
      warehouseMap: new Map(bodegas.map((item) => [item.id, item])),
    };
  }

  private mapConsumoWithCatalogs(
    row: ConsumoRepuestoEntity,
    productMap: Map<string, ProductoEntity>,
    warehouseMap: Map<string, BodegaEntity>,
  ) {
    const producto = productMap.get(row.producto_id);
    const bodega = row.bodega_id ? warehouseMap.get(row.bodega_id) : undefined;
    return {
      ...row,
      producto_codigo: producto?.codigo ?? null,
      producto_nombre: producto?.nombre ?? null,
      producto_label: this.buildProductoLabel(producto) ?? row.producto_id,
      bodega_codigo: bodega?.codigo ?? null,
      bodega_nombre: bodega?.nombre ?? null,
      bodega_label: this.buildBodegaLabel(bodega) ?? row.bodega_id ?? null,
    };
  }

  private mapIssueItemWithCatalogs(
    row: EntregaMaterialDetEntity,
    productMap: Map<string, ProductoEntity>,
    warehouseMap: Map<string, BodegaEntity>,
  ) {
    const producto = productMap.get(row.producto_id);
    const bodega = warehouseMap.get(row.bodega_id);
    return {
      ...row,
      producto_codigo: producto?.codigo ?? null,
      producto_nombre: producto?.nombre ?? null,
      producto_label: this.buildProductoLabel(producto) ?? row.producto_id,
      bodega_codigo: bodega?.codigo ?? null,
      bodega_nombre: bodega?.nombre ?? null,
      bodega_label: this.buildBodegaLabel(bodega) ?? row.bodega_id,
    };
  }

  private async validateProductoEnBodega(productoId: string, bodegaId: string) {
    const [producto, bodega, stock] = await Promise.all([
      this.productoRepo.findOne({ where: { id: productoId } }),
      this.bodegaRepo.findOne({ where: { id: bodegaId } }),
      this.stockRepo.findOne({ where: { producto_id: productoId, bodega_id: bodegaId } }),
    ]);

    if (!producto) {
      throw new NotFoundException('Producto no encontrado');
    }
    if (!bodega) {
      throw new NotFoundException('Bodega no encontrada');
    }
    if (!stock) {
      throw new BadRequestException('El producto seleccionado no pertenece a la bodega indicada.');
    }

    return { producto, bodega, stock };
  }

  private async resolveInventoryCostReference(productoId: string, bodegaId: string) {
    const { producto, bodega } = await this.validateProductoEnBodega(productoId, bodegaId);
    const kardex = await this.kardexRepo.findOne({
      where: { producto_id: productoId, bodega_id: bodegaId },
      order: { fecha: 'DESC', id: 'DESC' },
    });

    const costoUnitario = this.toNumeric(
      kardex?.costo_unitario ?? producto?.ultimo_costo ?? 0,
    );
    const saldoCostoPromedio = this.toNumeric(
      kardex?.saldo_costo_promedio ?? producto?.ultimo_costo ?? 0,
    );

    return {
      producto_id: productoId,
      bodega_id: bodegaId,
      producto_nombre: producto?.nombre ?? null,
      producto_codigo: producto?.codigo ?? null,
      producto_label: this.buildProductoLabel(producto) ?? productoId,
      bodega_nombre: bodega?.nombre ?? null,
      bodega_codigo: bodega?.codigo ?? null,
      bodega_label: this.buildBodegaLabel(bodega) ?? bodegaId,
      costo_unitario: costoUnitario,
      saldo_costo_promedio: saldoCostoPromedio,
      fuente: kardex ? 'KARDEX' : 'PRODUCTO',
      kardex_id: kardex?.id ?? null,
      fecha_kardex: kardex?.fecha ?? null,
    };
  }

  onModuleInit() {
    this.scheduleAlertRecalculation();
    this.triggerAlertRecalculation('startup').catch((e: any) => {
      this.logger.error(
        `No se pudo iniciar recálculo de alertas en startup: ${e?.message ?? 'desconocido'}`,
      );
    });
  }

  onModuleDestroy() {
    if (this.recalculationInterval) {
      clearInterval(this.recalculationInterval);
      this.recalculationInterval = null;
    }
  }

  private scheduleAlertRecalculation() {
    if (this.recalculationInterval) return;
    this.recalculationInterval = setInterval(() => {
      this.triggerAlertRecalculation('interval').catch((e: any) => {
        this.logger.error(
          `Error en recálculo automático de alertas: ${e?.message ?? 'desconocido'}`,
        );
      });
    }, this.RECALCULATION_INTERVAL_MS);
  }

  async triggerAlertRecalculation(source = 'manual') {
    if (this.recalculationRunning) {
      return this.wrap(
        { accepted: false, source },
        'Recalculo de alertas ya se encuentra en ejecución',
      );
    }

    this.recalculationRunning = true;
    void this.recalculateAlertas()
      .then((result) => {
        const stats = result.data as { total?: number; skipped?: number };
        this.logger.log(
          `Recalculo de alertas finalizado (${source}): total=${stats.total ?? 0}, skipped=${stats.skipped ?? 0}`,
        );
      })
      .catch((e: any) => {
        this.logger.error(
          `Error ejecutando recálculo de alertas (${source}): ${e?.message ?? 'desconocido'}`,
        );
      })
      .finally(() => {
        this.recalculationRunning = false;
      });

    return this.wrap(
      { accepted: true, source },
      'Recalculo de alertas ejecutándose en segundo plano',
    );
  }

  async recalculateAlertasNow(source = 'manual') {
    if (this.recalculationRunning) {
      return this.wrap(
        { accepted: false, source },
        'Recalculo de alertas ya se encuentra en ejecución',
      );
    }

    this.recalculationRunning = true;
    try {
      return await this.recalculateAlertas(source);
    } finally {
      this.recalculationRunning = false;
    }
  }

  private wrap(data: unknown, message = 'OK', meta?: unknown) {
    return { data, meta, message };
  }

  private normalizeAlertLevel(value: unknown): AlertLevel {
    const raw = String(value || '').trim().toUpperCase();
    if (
      [
        'CRITICAL',
        'CRITICA',
        'CRITICO',
        'ALTO',
        'HIGH',
        'ROJO',
        'ANORMAL',
      ].includes(raw)
    ) {
      return 'CRITICAL';
    }
    if (
      [
        'WARNING',
        'WARN',
        'ALERTA',
        'MEDIO',
        'MEDIA',
        'AMARILLO',
        'PRECAUCION',
      ].includes(raw)
    ) {
      return 'WARNING';
    }
    return 'INFO';
  }

  private normalizeAlertState(value: unknown) {
    const raw = String(value || '').trim().toUpperCase();
    if (['CERRADA', 'CLOSED'].includes(raw)) return 'CERRADA';
    if (['RESUELTA', 'RESOLVED'].includes(raw)) return 'RESUELTA';
    if (
      ['EN_PROCESO', 'EN PROCESO', 'IN_PROGRESS', 'IN PROGRESS'].includes(raw)
    ) {
      return 'EN_PROCESO';
    }
    return 'ABIERTA';
  }

  private alertStateRank(value: unknown) {
    const state = this.normalizeAlertState(value);
    if (state === 'ABIERTA') return 0;
    if (state === 'EN_PROCESO') return 1;
    if (state === 'RESUELTA') return 2;
    return 3;
  }

  private alertLevelRank(value: unknown) {
    const level = this.normalizeAlertLevel(value);
    if (level === 'CRITICAL') return 0;
    if (level === 'WARNING') return 1;
    return 2;
  }

  private buildAlertIdentity(input: {
    equipo_id?: string | null;
    tipo_alerta: string;
    referencia?: string | null;
    origen?: string | null;
  }) {
    return [
      String(input.origen || 'SYSTEM').trim().toUpperCase(),
      String(input.equipo_id || 'GLOBAL').trim() || 'GLOBAL',
      String(input.referencia || '').trim() || 'SIN_REFERENCIA',
    ].join('::');
  }

  private maxAlertLevel(...values: unknown[]) {
    const levels = values.map((value) => this.normalizeAlertLevel(value));
    if (levels.includes('CRITICAL')) return 'CRITICAL' as AlertLevel;
    if (levels.includes('WARNING')) return 'WARNING' as AlertLevel;
    return 'INFO' as AlertLevel;
  }

  private normalizeMaterialIdArray(values: unknown) {
    if (!Array.isArray(values)) return [];
    return [...new Set(values.map((value) => String(value || '').trim()).filter(Boolean))];
  }

  private normalizePlanTaskFieldType(value: unknown) {
    const raw = String(value || '').trim().toUpperCase();
    if (['BOOLEAN', 'BOOL', 'CHECKBOX', 'SI_NO', 'SI/NO'].includes(raw)) {
      return 'BOOLEAN';
    }
    if (['NUMBER', 'NUMERIC', 'DECIMAL', 'INTEGER', 'NUMERO'].includes(raw)) {
      return 'NUMBER';
    }
    if (['JSON', 'OBJECT', 'OBJETO', 'EVIDENCIA'].includes(raw)) {
      return 'JSON';
    }
    if (['TEXT', 'TEXTO', 'STRING'].includes(raw)) {
      return 'TEXT';
    }
    return null;
  }

  private resolvePlanTaskFieldTypeFromActividad(
    actividad: ProcedimientoActividadEntity,
  ) {
    const explicit = this.normalizePlanTaskFieldType(
      (actividad.meta as Record<string, unknown> | undefined)?.field_type,
    );
    if (explicit) return explicit;
    return actividad.requiere_evidencia ? 'JSON' : 'BOOLEAN';
  }

  private resolvePlanTaskRequiredFromActividad(
    actividad: ProcedimientoActividadEntity,
  ) {
    const explicit = (actividad.meta as Record<string, unknown> | undefined)
      ?.required;
    if (typeof explicit === 'boolean') return explicit;
    return true;
  }

  private extractAlertWorkOrderSnapshots(
    row: Pick<AlertaMantenimientoEntity, 'payload_json' | 'work_order_id'>,
  ) {
    const payload = (row.payload_json ?? {}) as Record<string, unknown>;
    const rawItems = Array.isArray(payload.work_orders) ? payload.work_orders : [];
    const snapshots = rawItems
      .map((item) =>
        item && typeof item === 'object'
          ? {
              id: String((item as Record<string, unknown>).id || '').trim(),
              code: String((item as Record<string, unknown>).code || '').trim() || null,
              title:
                String((item as Record<string, unknown>).title || '').trim() || null,
              status_workflow:
                String(
                  (item as Record<string, unknown>).status_workflow || '',
                ).trim() || null,
            }
          : null,
      )
      .filter((item): item is {
        id: string;
        code: string | null;
        title: string | null;
        status_workflow: string | null;
      } => Boolean(item?.id));

    if (
      row.work_order_id &&
      !snapshots.some((item) => item.id === row.work_order_id)
    ) {
      snapshots.push({
        id: row.work_order_id,
        code: null,
        title: null,
        status_workflow: null,
      });
    }

    return snapshots;
  }

  private buildAlertWorkOrderSnapshot(workOrder: Partial<WorkOrderEntity>) {
    return {
      id: String(workOrder.id || '').trim(),
      code: String(workOrder.code || '').trim() || null,
      title: String(workOrder.title || '').trim() || null,
      status_workflow: this.normalizeWorkflowStatus(workOrder.status_workflow),
    };
  }

  private hasLinkedWorkOrders(
    row: Pick<AlertaMantenimientoEntity, 'payload_json' | 'work_order_id'>,
  ) {
    return this.extractAlertWorkOrderSnapshots(row).length > 0;
  }

  private resolveAlertStateFromLinkedWorkOrders(
    snapshots: Array<{ status_workflow: string | null }>,
    fallbackState: string,
  ) {
    if (!snapshots.length) return fallbackState;
    const normalizedStatuses = snapshots.map((item) =>
      this.normalizeWorkflowStatus(item.status_workflow),
    );
    if (normalizedStatuses.some((status) => status !== 'CLOSED')) {
      return 'EN_PROCESO';
    }
    return 'CERRADA';
  }

  private async syncAlertWorkOrderLink(
    alertaId: string,
    workOrder: WorkOrderEntity,
    nextAlertState?: string,
  ) {
    const alerta = await this.findOneOrFail(this.alertaRepo, {
      id: alertaId,
      is_deleted: false,
    });
    const payload = {
      ...((alerta.payload_json ?? {}) as Record<string, unknown>),
    };
    const nextSnapshot = this.buildAlertWorkOrderSnapshot(workOrder);
    const snapshots = this.extractAlertWorkOrderSnapshots(alerta).filter(
      (item) => item.id !== nextSnapshot.id,
    );
    snapshots.push(nextSnapshot);

    payload.work_orders = snapshots;
    alerta.payload_json = payload;
    alerta.work_order_id = workOrder.id;
    alerta.estado =
      nextAlertState ??
      this.resolveAlertStateFromLinkedWorkOrders(snapshots, alerta.estado);
    return this.alertaRepo.save(alerta);
  }

  private async syncAlertsForWorkOrder(workOrder: WorkOrderEntity) {
    const alertas = await this.alertaRepo.find({
      where: { is_deleted: false },
      order: { fecha_generada: 'DESC', id: 'DESC' },
    });

    const linked = alertas.filter((alerta) =>
      this.extractAlertWorkOrderSnapshots(alerta).some(
        (item) => item.id === workOrder.id,
      ),
    );

    if (!linked.length) return;

    for (const alerta of linked) {
      const payload = {
        ...((alerta.payload_json ?? {}) as Record<string, unknown>),
      };
      const snapshots = this.extractAlertWorkOrderSnapshots(alerta).map((item) =>
        item.id === workOrder.id ? this.buildAlertWorkOrderSnapshot(workOrder) : item,
      );
      payload.work_orders = snapshots;
      alerta.payload_json = payload;
      alerta.work_order_id = snapshots[snapshots.length - 1]?.id ?? null;
      alerta.estado = this.resolveAlertStateFromLinkedWorkOrders(
        snapshots,
        alerta.estado,
      );
      await this.alertaRepo.save(alerta);
    }
  }


  private readonly notificationServiceUrl = (
    process.env.NOTIFICATION_SERVICE_URL || process.env.KPI_NOTIFICATION_URL || ''
  ).replace(/\/$/, '');

  private readonly securityServiceUrl = (
    process.env.SECURITY_SERVICE_URL || process.env.KPI_SECURITY_URL || ''
  ).replace(/\/$/, '');

  private readonly publicBaseUrl = (
    process.env.PUBLIC_BASE_URL || process.env.APP_PUBLIC_URL || ''
  ).replace(/\/$/, '');

  private toNumeric(value: unknown, fallback = 0) {
    const num = Number(value);
    return Number.isFinite(num) ? num : fallback;
  }

  private normalizeWorkflowStatus(value: unknown) {
    const raw = String(value || '').trim().toUpperCase();
    if (['PLANNED', 'PLANIFICADA', 'PLANIFICADO', 'CREADA', 'CREADO'].includes(raw)) return 'PLANNED';
    if (['IN_PROGRESS', 'IN PROGRESS', 'EN_PROCESO', 'EN PROCESO', 'PROCESSING'].includes(raw)) return 'IN_PROGRESS';
    if (['CLOSED', 'CERRADA', 'CERRADO', 'DONE', 'COMPLETED'].includes(raw)) return 'CLOSED';
    return raw || 'PLANNED';
  }

  private incrementWorkOrderPrefix(letter: string) {
    const nextCharCode = letter.toUpperCase().charCodeAt(0) + 1;
    if (nextCharCode > 90) return 'A';
    return String.fromCharCode(nextCharCode);
  }

  private computeNextWorkOrderCode(lastCode: string | null) {
    if (!lastCode) return 'OT-A00001';
    const match = /^OT-([A-Z])(\d{5})$/i.exec(String(lastCode).trim());
    if (!match) return 'OT-A00001';
    const currentLetter = (match[1] ?? 'A').toUpperCase();
    const currentNumber = Number(match[2] ?? '0');
    if (currentNumber >= 99999) {
      return `OT-${this.incrementWorkOrderPrefix(currentLetter)}00001`;
    }
    return `OT-${currentLetter}${String(currentNumber + 1).padStart(5, '0')}`;
  }

  private getWorkOrderCodeRank(code: string) {
    const match = /^OT-([A-Z])(\d{5})$/i.exec(String(code || '').trim());
    if (!match) return -1;
    const letter = (match[1] ?? 'A').toUpperCase();
    const number = Number(match[2] ?? '0');
    return (letter.charCodeAt(0) - 64) * 100000 + number;
  }

  private computeNextAlphaNumericCode(prefix: string, lastCode: string | null) {
    if (!lastCode) return `${prefix}-A00001`;
    const pattern = new RegExp(`^${prefix}-([A-Z])(\\d{5})$`, 'i');
    const match = pattern.exec(String(lastCode).trim());
    if (!match) return `${prefix}-A00001`;
    const currentLetter = (match[1] ?? 'A').toUpperCase();
    const currentNumber = Number(match[2] ?? '0');
    if (currentNumber >= 99999) {
      return `${prefix}-${this.incrementWorkOrderPrefix(currentLetter)}00001`;
    }
    return `${prefix}-${currentLetter}${String(currentNumber + 1).padStart(5, '0')}`;
  }

  private getAlphaNumericCodeRank(prefix: string, code: string) {
    const pattern = new RegExp(`^${prefix}-([A-Z])(\\d{5})$`, 'i');
    const match = pattern.exec(String(code || '').trim());
    if (!match) return -1;
    const letter = (match[1] ?? 'A').toUpperCase();
    const number = Number(match[2] ?? '0');
    return (letter.charCodeAt(0) - 64) * 100000 + number;
  }

  private async generateNextWorkOrderCode() {
    const rows = await this.woRepo.find({
      select: { code: true, id: true },
    });
    const codes = rows
      .map((row) => String(row.code || '').trim())
      .filter(Boolean)
      .sort((a, b) => this.getWorkOrderCodeRank(b) - this.getWorkOrderCodeRank(a));
    return this.computeNextWorkOrderCode(codes[0] ?? null);
  }

  private async generateNextProcedimientoPlantillaCode() {
    const rows = await this.procedimientoRepo.find({
      select: { codigo: true, id: true },
    });
    const codes = rows
      .map((row) => String(row.codigo || '').trim())
      .filter(Boolean)
      .sort(
        (a, b) =>
          this.getAlphaNumericCodeRank('PMP', b) -
          this.getAlphaNumericCodeRank('PMP', a),
      );
    return this.computeNextAlphaNumericCode('PMP', codes[0] ?? null);
  }

  private async generateNextAnalisisLubricanteCode() {
    const rows = await this.analisisLubricanteRepo.find({
      select: { codigo: true, id: true },
    });
    const codes = rows
      .map((row) => String(row.codigo || '').trim())
      .filter(Boolean)
      .sort(
        (a, b) =>
          this.getAlphaNumericCodeRank('AL', b) -
          this.getAlphaNumericCodeRank('AL', a),
      );
    return this.computeNextAlphaNumericCode('AL', codes[0] ?? null);
  }

  async getNextWorkOrderCode() {
    return this.wrap({ code: await this.generateNextWorkOrderCode() }, 'Siguiente código de OT generado');
  }

  async getNextProcedimientoPlantillaCode() {
    return this.wrap(
      { code: await this.generateNextProcedimientoPlantillaCode() },
      'Siguiente código de plantilla MPG generado',
    );
  }

  async getNextAnalisisLubricanteCode() {
    return this.wrap(
      { code: await this.generateNextAnalisisLubricanteCode() },
      'Siguiente código de análisis de lubricante generado',
    );
  }

  private async resolveRequestedWorkOrderCode(
    requestedCode?: string | null,
  ): Promise<CodeResolution> {
    const candidate = String(requestedCode || '').trim();
    if (!candidate) {
      const generatedCode = await this.generateNextWorkOrderCode();
      return {
        requestedCode: null,
        resolvedCode: generatedCode,
        codeWasReassigned: false,
        reassignmentReason: null,
      };
    }
    const existing = await this.woRepo.findOne({ where: { code: candidate } });
    if (!existing) {
      return {
        requestedCode: candidate,
        resolvedCode: candidate,
        codeWasReassigned: false,
        reassignmentReason: null,
      };
    }
    const generatedCode = await this.generateNextWorkOrderCode();
    return {
      requestedCode: candidate,
      resolvedCode: generatedCode,
      codeWasReassigned: generatedCode !== candidate,
      reassignmentReason: existing.is_deleted
        ? 'El código solicitado existía en una OT eliminada lógicamente.'
        : 'El código solicitado ya estaba en uso.',
    };
  }

  private async resolveRequestedProcedimientoPlantillaCode(
    requestedCode?: string | null,
  ): Promise<CodeResolution> {
    const candidate = String(requestedCode || '').trim();
    if (!candidate) {
      const generatedCode = await this.generateNextProcedimientoPlantillaCode();
      return {
        requestedCode: null,
        resolvedCode: generatedCode,
        codeWasReassigned: false,
        reassignmentReason: null,
      };
    }
    const existing = await this.procedimientoRepo.findOne({
      where: { codigo: candidate },
    });
    if (!existing) {
      return {
        requestedCode: candidate,
        resolvedCode: candidate,
        codeWasReassigned: false,
        reassignmentReason: null,
      };
    }
    const generatedCode = await this.generateNextProcedimientoPlantillaCode();
    return {
      requestedCode: candidate,
      resolvedCode: generatedCode,
      codeWasReassigned: generatedCode !== candidate,
      reassignmentReason: existing.is_deleted
        ? 'El código solicitado existía en una plantilla eliminada lógicamente.'
        : 'El código solicitado ya estaba en uso.',
    };
  }

  private async resolveRequestedAnalisisLubricanteCode(
    requestedCode?: string | null,
  ): Promise<CodeResolution> {
    const candidate = String(requestedCode || '').trim();
    if (!candidate) {
      const generatedCode = await this.generateNextAnalisisLubricanteCode();
      return {
        requestedCode: null,
        resolvedCode: generatedCode,
        codeWasReassigned: false,
        reassignmentReason: null,
      };
    }
    const existing = await this.analisisLubricanteRepo.findOne({
      where: { codigo: candidate },
    });
    if (!existing) {
      return {
        requestedCode: candidate,
        resolvedCode: candidate,
        codeWasReassigned: false,
        reassignmentReason: null,
      };
    }
    const generatedCode = await this.generateNextAnalisisLubricanteCode();
    return {
      requestedCode: candidate,
      resolvedCode: generatedCode,
      codeWasReassigned: generatedCode !== candidate,
      reassignmentReason: existing.is_deleted
        ? 'El código solicitado existía en un análisis eliminado lógicamente.'
        : 'El código solicitado ya estaba en uso.',
    };
  }

  private isDuplicateWorkOrderCodeError(error: any) {
    const driverCode = String(error?.driverError?.code || error?.code || '').trim();
    const constraint = String(error?.driverError?.constraint || error?.constraint || '').trim();
    return driverCode === '23505' && constraint === 'tb_work_order_code_key';
  }

  private isDuplicateCodigoError(error: any) {
    const driverCode = String(
      error?.driverError?.code || error?.code || '',
    ).trim();
    const constraint = String(
      error?.driverError?.constraint || error?.constraint || '',
    )
      .trim()
      .toLowerCase();
    return (
      driverCode === '23505' &&
      (constraint.includes('codigo') || constraint.includes('code'))
    );
  }

  private buildMaintenanceRelativePath(path: string) {
    const basePath = String(process.env.BASE_PATH || '/kpi_maintenance').replace(/\/$/, '');
    return `${basePath}${path.startsWith('/') ? path : `/${path}`}`;
  }

  private buildMaintenancePublicUrl(path: string) {
    const relative = this.buildMaintenanceRelativePath(path);
    return this.publicBaseUrl ? `${this.publicBaseUrl}${relative}` : relative;
  }

  private async postJson(url: string, payload: Record<string, unknown>) {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      const body = await response.text().catch(() => '');
      throw new Error(`HTTP ${response.status}: ${body || response.statusText}`);
    }
    return response;
  }

  private async publishInAppNotification(payload: {
    title: string;
    body: string;
    module?: string;
    entityType?: string;
    entityId?: string | null;
    level?: string;
    recipients?: string[];
  }) {
    if (!this.notificationServiceUrl) return;
    try {
      await this.postJson(`${this.notificationServiceUrl}/notifications/in-app`, {
        title: payload.title,
        body: payload.body,
        module: payload.module ?? 'maintenance',
        entityType: payload.entityType ?? 'work-order',
        entityId: payload.entityId ?? null,
        level: payload.level ?? 'info',
        recipients: payload.recipients ?? [],
      });
    } catch (error: any) {
      this.logger.warn(`No se pudo publicar notificación: ${error?.message ?? 'desconocido'}`);
    }
  }

  private async writeSecurityLog(payload: {
    description: string;
    status?: string;
    typeLog?: string;
    createdBy?: string | null;
  }) {
    if (!this.securityServiceUrl) return;
    try {
      await this.postJson(`${this.securityServiceUrl}/log-transacts`, {
        moduleMicroservice: 'kpi_maintenance',
        status: payload.status ?? 'SUCCESS',
        typeLog: payload.typeLog ?? 'WORK_ORDER',
        description: payload.description,
        createdBy: payload.createdBy ?? null,
      });
    } catch (error: any) {
      this.logger.warn(`No se pudo registrar log transaccional: ${error?.message ?? 'desconocido'}`);
    }
  }

  private normalizeStringArray(values?: string[] | null) {
    if (!Array.isArray(values)) return [];
    return values
      .map((value) => String(value || '').trim())
      .filter(Boolean);
  }

  private buildProcedimientoPlanCode(row: ProcedimientoPlantillaEntity) {
    const base = String(row.codigo || row.id)
      .trim()
      .toUpperCase()
      .replace(/[^A-Z0-9_-]+/g, '-')
      .replace(/-+/g, '-')
      .replace(/^-|-$/g, '');
    return `MPGTPL-${base || row.id}`;
  }

  private buildProcedimientoPlanDescription(row: ProcedimientoPlantillaEntity) {
    const details = [
      `[PROCEDIMIENTO:${row.id}]`,
      row.documento_referencia ? `DOC:${row.documento_referencia}` : '',
      row.version ? `VER:${row.version}` : '',
      row.objetivo ?? '',
    ]
      .map((value) => String(value || '').trim())
      .filter(Boolean);

    return details.join(' | ');
  }

  private extractProcedimientoIdFromPlan(plan?: { descripcion?: string | null } | null) {
    const match = String(plan?.descripcion || '').match(
      /\[PROCEDIMIENTO:([0-9a-fA-F-]{36})\]/,
    );
    return match?.[1] ?? null;
  }

  private async resolveProcedimientoFromPlan(
    plan?: PlanMantenimientoEntity | null,
  ) {
    const procedimientoId = this.extractProcedimientoIdFromPlan(plan);
    if (!procedimientoId) return null;
    return this.procedimientoRepo.findOne({
      where: { id: procedimientoId, is_deleted: false },
    });
  }

  private buildPlanTaskMetaFromProcedimiento(
    procedimiento: ProcedimientoPlantillaEntity,
    actividad: ProcedimientoActividadEntity,
  ) {
    const actividadMeta =
      (actividad.meta as Record<string, unknown> | undefined) ?? {};
    const evidencias = Array.isArray(actividadMeta.evidencias_requeridas)
      ? actividadMeta.evidencias_requeridas
      : [];
    const fieldType = this.resolvePlanTaskFieldTypeFromActividad(actividad);
    const required = this.resolvePlanTaskRequiredFromActividad(actividad);

    return {
      fuente: 'PROCEDIMIENTO_MPG',
      procedimiento_id: procedimiento.id,
      procedimiento_codigo: procedimiento.codigo,
      procedimiento_nombre: procedimiento.nombre,
      procedimiento_actividad_id: actividad.id,
      fase: actividad.fase ?? null,
      detalle: actividad.detalle ?? null,
      requiere_permiso: actividad.requiere_permiso ?? false,
      requiere_epp: actividad.requiere_epp ?? false,
      requiere_bloqueo: actividad.requiere_bloqueo ?? false,
      requiere_evidencia: actividad.requiere_evidencia ?? false,
      evidencias_requeridas: evidencias,
      field_type: fieldType,
      required,
      documento_referencia: procedimiento.documento_referencia ?? null,
      version: procedimiento.version ?? null,
    };
  }

  private async syncPlanFromProcedimiento(procedimientoId: string) {
    const procedimiento = await this.findOneOrFail(this.procedimientoRepo, {
      id: procedimientoId,
      is_deleted: false,
    });
    const actividades = await this.procedimientoActividadRepo.find({
      where: { procedimiento_id: procedimiento.id, is_deleted: false },
      order: { orden: 'ASC', created_at: 'ASC' },
    });

    const planCode = this.buildProcedimientoPlanCode(procedimiento);
    let plan = await this.planRepo.findOne({
      where: { codigo: planCode, is_deleted: false },
    });

    if (!plan) {
      plan = this.planRepo.create({
        codigo: planCode,
        nombre: procedimiento.nombre,
        tipo:
          procedimiento.clase_mantenimiento ??
          procedimiento.tipo_proceso ??
          'PREVENTIVO',
        descripcion: this.buildProcedimientoPlanDescription(procedimiento),
        frecuencia_tipo: 'HORAS',
        frecuencia_valor: procedimiento.frecuencia_horas ?? 0,
        requiere_parada: actividades.some(
          (actividad) =>
            Boolean(actividad.requiere_permiso) ||
            Boolean(actividad.requiere_bloqueo),
        ),
      });
    } else {
      Object.assign(plan, {
        nombre: procedimiento.nombre,
        tipo:
          procedimiento.clase_mantenimiento ??
          procedimiento.tipo_proceso ??
          plan.tipo,
        descripcion: this.buildProcedimientoPlanDescription(procedimiento),
        frecuencia_tipo: 'HORAS',
        frecuencia_valor: procedimiento.frecuencia_horas ?? 0,
        requiere_parada: actividades.some(
          (actividad) =>
            Boolean(actividad.requiere_permiso) ||
            Boolean(actividad.requiere_bloqueo),
        ),
      });
    }

    plan = await this.planRepo.save(plan);

    const existingTasks = await this.planTareaRepo.find({
      where: { plan_id: plan.id, is_deleted: false },
      order: { orden: 'ASC' },
    });
    const existingByActividadId = new Map<string, PlanTareaEntity>();
    const existingByOrder = new Map<number, PlanTareaEntity>();
    for (const task of existingTasks) {
      const actividadId = String(
        (task.meta as Record<string, unknown> | undefined)
          ?.procedimiento_actividad_id ?? '',
      ).trim();
      if (actividadId) existingByActividadId.set(actividadId, task);
      if (!existingByOrder.has(task.orden)) existingByOrder.set(task.orden, task);
    }

    const tasksToSave: PlanTareaEntity[] = [];
    const keptTaskIds = new Set<string>();

    for (const [index, actividad] of actividades.entries()) {
      const meta = this.buildPlanTaskMetaFromProcedimiento(
        procedimiento,
        actividad,
      );
      const fieldType = this.resolvePlanTaskFieldTypeFromActividad(actividad);
      const required = this.resolvePlanTaskRequiredFromActividad(actividad);
      const existingTask =
        existingByActividadId.get(String(actividad.id)) ??
        existingByOrder.get(actividad.orden ?? index + 1);

      if (existingTask) {
        existingTask.orden = actividad.orden ?? index + 1;
        existingTask.actividad = actividad.actividad;
        existingTask.field_type = fieldType;
        existingTask.required = required;
        existingTask.meta = meta;
        existingTask.status = 'ACTIVE';
        existingTask.is_deleted = false;
        tasksToSave.push(existingTask);
        keptTaskIds.add(existingTask.id);
        continue;
      }

      tasksToSave.push(
        this.planTareaRepo.create({
          plan_id: plan.id,
          orden: actividad.orden ?? index + 1,
          actividad: actividad.actividad,
          field_type: fieldType,
          required,
          meta,
        }),
      );
    }

    if (tasksToSave.length) {
      await this.planTareaRepo.save(tasksToSave);
    }

    const tasksToDelete = existingTasks.filter(
      (task) => !keptTaskIds.has(task.id),
    );
    if (tasksToDelete.length) {
      for (const task of tasksToDelete) task.is_deleted = true;
      await this.planTareaRepo.save(tasksToDelete);
    }

    return { plan, procedimiento, actividades };
  }

  private toDateOnlyString(value?: string | Date | null) {
    if (!value) return null;
    return new Date(value).toISOString().slice(0, 10);
  }

  private toTimeOnlyString(value?: string | null) {
    if (!value) return null;
    const normalized = String(value).trim();
    if (!normalized) return null;
    return normalized.length >= 8 ? normalized.slice(0, 8) : normalized;
  }

  private async softDeleteRows<T extends ObjectLiteral & { is_deleted: boolean }>(
    repo: Repository<T>,
    where: FindOptionsWhere<T>,
  ) {
    const rows = await repo.find({ where });
    if (!rows.length) return;
    for (const row of rows) row.is_deleted = true;
    await repo.save(rows);
  }

  private async registerProcessEvent(payload: {
    tipo_proceso: string;
    accion: string;
    referencia_tabla: string;
    referencia_id?: string | null;
    referencia_codigo?: string | null;
    equipo_id?: string | null;
    title: string;
    body: string;
    level?: string;
    payload_kpi?: Record<string, unknown>;
    created_by?: string | null;
  }) {
    const notificationPayload = {
      title: payload.title,
      body: payload.body,
      module: 'maintenance-intelligence',
      entityType: payload.tipo_proceso,
      entityId: payload.referencia_id ?? null,
      level: payload.level ?? 'info',
    };

    const event = await this.eventoProcesoRepo.save(
      this.eventoProcesoRepo.create({
        tipo_proceso: payload.tipo_proceso,
        accion: payload.accion,
        referencia_tabla: payload.referencia_tabla,
        referencia_id: payload.referencia_id ?? null,
        referencia_codigo: payload.referencia_codigo ?? null,
        equipo_id: payload.equipo_id ?? null,
        estado: 'COMPLETED',
        notificacion_enviada: false,
        payload_notificacion: notificationPayload,
        payload_kpi: payload.payload_kpi ?? {},
        created_by: payload.created_by ?? null,
      }),
    );

    await this.publishInAppNotification({
      title: payload.title,
      body: payload.body,
      module: 'maintenance-intelligence',
      entityType: payload.tipo_proceso,
      entityId: payload.referencia_id ?? null,
      level: payload.level ?? 'info',
    });

    event.notificacion_enviada = true;
    await this.eventoProcesoRepo.save(event);
    return event;
  }

  private async buildProcedimientoPayload(row: ProcedimientoPlantillaEntity) {
    const planCode = this.buildProcedimientoPlanCode(row);
    const materialIds = this.normalizeMaterialIdArray(row.materiales);
    const [actividades, plan, materialesCatalogo] = await Promise.all([
      this.procedimientoActividadRepo.find({
        where: { procedimiento_id: row.id, is_deleted: false },
        order: { orden: 'ASC', created_at: 'ASC' },
      }),
      this.planRepo.findOne({
        where: { codigo: planCode, is_deleted: false },
      }),
      materialIds.length
        ? this.productoRepo.find({
            where: { id: In(materialIds) },
          })
        : Promise.resolve([] as ProductoEntity[]),
    ]);

    const planTareas = plan
      ? await this.planTareaRepo.find({
          where: { plan_id: plan.id, is_deleted: false },
          order: { orden: 'ASC' },
        })
      : [];

    const materialesMap = new Map(
      materialesCatalogo.map((material) => [material.id, material]),
    );

    return {
      ...row,
      actividades,
      plan_id: plan?.id ?? null,
      plan_codigo: plan?.codigo ?? null,
      plan_nombre: plan?.nombre ?? null,
      plan_tareas: planTareas,
      materiales_detalle: materialIds
        .map((materialId) => materialesMap.get(materialId))
        .filter((material): material is ProductoEntity => Boolean(material))
        .map((material) => ({
          id: material.id,
          codigo: material.codigo ?? null,
          nombre: material.nombre ?? null,
          label: this.buildProductoLabel(material) ?? material.id,
        })),
    };
  }

  private trimNullableText(value: unknown) {
    const trimmed = String(value ?? '').trim();
    return trimmed || null;
  }

  private normalizeWorkOrderTaskPayload(
    definition: Pick<PlanTareaEntity, 'actividad' | 'field_type' | 'required'>,
    dto: Pick<
      CreateWorkOrderTareaDto | UpdateWorkOrderTareaDto,
      'valor_boolean' | 'valor_numeric' | 'valor_text' | 'valor_json' | 'observacion'
    >,
  ) {
    const fieldType =
      this.normalizePlanTaskFieldType(definition.field_type) ?? 'BOOLEAN';
    const required = Boolean(definition.required);

    let valor_boolean: boolean | null = null;
    let valor_numeric: number | null = null;
    let valor_text: string | null = null;
    let valor_json: Record<string, unknown> | null = null;

    if (fieldType === 'BOOLEAN') {
      if (dto.valor_boolean === true || dto.valor_boolean === false) {
        valor_boolean = dto.valor_boolean;
      }
      if (required && valor_boolean === null) {
        throw new BadRequestException(
          `La tarea ${definition.actividad} requiere una captura booleana.`,
        );
      }
    } else if (fieldType === 'NUMBER') {
      if (dto.valor_numeric !== undefined && dto.valor_numeric !== null) {
        const numericValue = Number(dto.valor_numeric);
        if (!Number.isFinite(numericValue)) {
          throw new BadRequestException(
            `La tarea ${definition.actividad} requiere un valor numérico válido.`,
          );
        }
        valor_numeric = numericValue;
      }
      if (required && valor_numeric === null) {
        throw new BadRequestException(
          `La tarea ${definition.actividad} requiere un valor numérico.`,
        );
      }
    } else if (fieldType === 'TEXT') {
      valor_text = this.trimNullableText(dto.valor_text);
      if (required && !valor_text) {
        throw new BadRequestException(
          `La tarea ${definition.actividad} requiere un texto.`,
        );
      }
    } else {
      if (dto.valor_json !== undefined && dto.valor_json !== null) {
        if (
          Array.isArray(dto.valor_json) ||
          typeof dto.valor_json !== 'object'
        ) {
          throw new BadRequestException(
            `La tarea ${definition.actividad} requiere un objeto JSON válido.`,
          );
        }
        valor_json = dto.valor_json;
      }
      if (required && !valor_json) {
        throw new BadRequestException(
          `La tarea ${definition.actividad} requiere una captura JSON.`,
        );
      }
    }

    return {
      valor_boolean,
      valor_numeric,
      valor_text,
      valor_json,
      observacion: this.trimNullableText(dto.observacion),
      field_type: fieldType,
      required,
    };
  }

  private async enrichWorkOrderTareas(rows: WorkOrderTareaEntity[]) {
    if (!rows.length) return [];

    const definitionIds = [
      ...new Set(rows.map((row) => row.tarea_id).filter(Boolean)),
    ] as string[];
    const definitions = definitionIds.length
      ? await this.planTareaRepo.find({
          where: { id: In(definitionIds), is_deleted: false },
        })
      : [];
    const definitionMap = new Map(definitions.map((row) => [row.id, row]));

    return [...rows]
      .map((row) => {
        const definition = definitionMap.get(row.tarea_id);
        return {
          ...row,
          orden: definition?.orden ?? null,
          actividad: definition?.actividad ?? null,
          field_type:
            definition?.field_type ??
            (definition
              ? this.normalizePlanTaskFieldType(definition.field_type)
              : null),
          required: definition?.required ?? false,
          task_meta: definition?.meta ?? {},
        };
      })
      .sort((a, b) => {
        const orderDiff = Number(a.orden ?? 999999) - Number(b.orden ?? 999999);
        if (orderDiff !== 0) return orderDiff;
        return String(a.id || '').localeCompare(String(b.id || ''));
      });
  }

  private normalizeSearchToken(value: unknown) {
    return String(value ?? '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-zA-Z0-9]+/g, ' ')
      .trim()
      .toLowerCase();
  }

  private safeDateOnlyString(value: unknown) {
    if (value == null || value === '') return null;
    const raw = String(value).trim();
    if (!raw) return null;
    const parsed = new Date(raw);
    if (Number.isNaN(parsed.getTime())) return raw;
    return parsed.toISOString().slice(0, 10);
  }

  private getLubricantMetricDefinition(
    parametro: unknown,
  ): LubricantMetricDefinition | null {
    const normalized = this.normalizeSearchToken(parametro);
    if (!normalized) return null;
    return (
      LUBRICANT_METRIC_DEFINITIONS.find((item) => {
        const terms = [item.label, item.key, ...(item.aliases ?? [])];
        return terms.some(
          (term) => this.normalizeSearchToken(term) === normalized,
        );
      }) ?? null
    );
  }

  private normalizeLubricantDetailLevel(value: unknown) {
    const raw = this.normalizeSearchToken(value);
    if (
      ['ALERTA', 'ANORMAL', 'CRITICO', 'CRÍTICO', 'CRITICAL', 'ROJO'].includes(
        raw,
      )
    ) {
      return 'ANORMAL';
    }
    if (
      [
        'OBSERVACION',
        'OBSERVACIÓN',
        'PRECAUCION',
        'PRECAUCIÓN',
        'WARNING',
        'WARN',
        'AMARILLO',
      ].includes(raw)
    ) {
      return 'PRECAUCION';
    }
    if (['N/D', 'ND', 'NO DISPONIBLE', 'SIN DATO', 'SIN DATOS'].includes(raw)) {
      return 'N/D';
    }
    return 'NORMAL';
  }

  private normalizeLubricantCondition(value: unknown) {
    const raw = this.normalizeSearchToken(value);
    if (['ANORMAL', 'ALERTA', 'CRITICO', 'CRITICO', 'CRITICAL'].includes(raw)) {
      return 'ANORMAL';
    }
    if (
      ['PRECAUCION', 'OBSERVACION', 'WARNING', 'WARN'].includes(raw)
    ) {
      return 'PRECAUCION';
    }
    if (['N/D', 'ND', 'NO DISPONIBLE', 'SIN EVALUACION'].includes(raw)) {
      return 'N/D';
    }
    return 'NORMAL';
  }

  private lubricantMetricUsesTextResult(
    definition?: LubricantMetricDefinition | null,
  ) {
    return ['HUMEDAD', 'COMBUSTIBLE'].includes(String(definition?.key || ''));
  }

  private hasLubricantDetailValue(
    detalle: Partial<AnalisisLubricanteDetalleEntity> | null | undefined,
  ) {
    if (!detalle) return false;
    if (
      detalle.resultado_numerico != null &&
      Number.isFinite(Number(detalle.resultado_numerico))
    ) {
      return true;
    }
    return String(detalle.resultado_texto ?? '').trim() !== '';
  }

  private buildLubricantEvaluationLabel(value: unknown) {
    const normalized = this.normalizeLubricantCondition(value);
    if (normalized === 'ANORMAL') return 'PARAMETROS ANORMALES';
    if (normalized === 'PRECAUCION') return 'PRECAUCION';
    if (normalized === 'N/D') return 'SIN EVALUACION';
    return 'PARAMETROS NORMALES';
  }

  private buildLubricantDiagnosticText(
    detalles: Partial<AnalisisLubricanteDetalleEntity>[],
    condition?: string | null,
  ) {
    const normalizedCondition = this.normalizeLubricantCondition(condition);
    const hasValues = detalles.some((detalle) => this.hasLubricantDetailValue(detalle));
    if (!hasValues && normalizedCondition === 'N/D') {
      return 'SIN EVALUACION';
    }
    return this.buildLubricantEvaluationLabel(normalizedCondition);
  }

  private async resolveAnalisisEquipmentContext(equipoId?: string | null) {
    if (!equipoId) {
      return {
        equipo: null as EquipoEntity | null,
        marcaNombre: null as string | null,
      };
    }

    const equipo = await this.equipoRepo.findOne({
      where: { id: equipoId, is_deleted: false },
    });
    if (!equipo) {
      throw new NotFoundException('Equipo no encontrado');
    }

    const marca = equipo.marca_id
      ? await this.marcaRepo.findOne({
          where: { id: equipo.marca_id, is_deleted: false },
        })
      : null;

    return {
      equipo,
      marcaNombre: marca?.nombre?.trim() || null,
    };
  }

  private mergeLubricantSampleInfo(
    payload: Record<string, unknown>,
    sampleInfo: Record<string, unknown>,
  ) {
    const nextPayload = { ...payload };
    nextPayload.sample_info = {
      ...(((payload.sample_info ?? payload.muestra) || {}) as Record<string, unknown>),
      ...sampleInfo,
    };
    return nextPayload;
  }

  private resolveLubricantIdentity(
    row:
      | (Partial<AnalisisLubricanteEntity> & {
          payload_json?: Record<string, unknown> | null;
        })
      | null
      | undefined,
  ) {
    const payload = (row?.payload_json ?? {}) as Record<string, unknown>;
    const sampleInfo = ((payload.sample_info ?? payload.muestra) ||
      {}) as Record<string, unknown>;

    const lubricante =
      String(
        row?.lubricante ??
          sampleInfo.lubricante ??
          payload.lubricante ??
          row?.equipo_codigo ??
          '',
      ).trim() || null;
    const marcaLubricante =
      String(
        row?.marca_lubricante ??
          sampleInfo.marca_lubricante ??
          payload.marca_lubricante ??
          row?.equipo_nombre ??
          '',
      ).trim() || null;
    const lubricanteCodigo =
      String(
        sampleInfo.lubricante_codigo ?? payload.lubricante_codigo ?? '',
      ).trim() || null;

    return {
      lubricante,
      marca_lubricante: marcaLubricante,
      lubricante_codigo: lubricanteCodigo,
      lubricante_label: [lubricanteCodigo, lubricante, marcaLubricante]
        .filter(Boolean)
        .join(' · '),
      lubricante_lookup_key: this.normalizeSearchToken(
        [lubricanteCodigo, lubricante, marcaLubricante].filter(Boolean).join(' '),
      ),
    };
  }

  private extractAnalysisSampleInfo(payload: Record<string, unknown>) {
    const info = ((payload.sample_info ?? payload.muestra) ||
      {}) as Record<string, unknown>;
    const toNullableNumber = (value: unknown) => {
      if (value == null || value === '') return null;
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : null;
    };

    return {
      numero_muestra:
        String(info.numero_muestra ?? payload.numero_muestra ?? '').trim() ||
        null,
      fecha_ingreso: this.safeDateOnlyString(
        info.fecha_ingreso ?? payload.fecha_ingreso,
      ),
      fecha_informe: this.safeDateOnlyString(
        info.fecha_informe ?? payload.fecha_informe,
      ),
      horas_equipo: toNullableNumber(
        info.horas_equipo ?? payload.horas_equipo,
      ),
      horas_lubricante: toNullableNumber(
        info.horas_lubricante ?? payload.horas_lubricante,
      ),
      condicion:
        String(info.condicion ?? payload.condicion ?? '').trim() !== ''
          ? this.normalizeLubricantCondition(info.condicion ?? payload.condicion)
          : 'N/D',
      equipo_marca:
        String(info.equipo_marca ?? payload.equipo_marca ?? '').trim() || null,
      equipo_serie:
        String(info.equipo_serie ?? payload.equipo_serie ?? '').trim() || null,
      equipo_modelo:
        String(info.equipo_modelo ?? payload.equipo_modelo ?? '').trim() || null,
      laboratorio:
        String(info.laboratorio ?? payload.laboratorio ?? '').trim() || null,
    };
  }

  private evaluateLubricantDetailLevel(
    detalle: Partial<AnalisisLubricanteDetalleEntity>,
    baselineValue?: number | null,
  ) {
    if (detalle.nivel_alerta) {
      return this.normalizeLubricantDetailLevel(detalle.nivel_alerta);
    }

    const definition = this.getLubricantMetricDefinition(detalle.parametro);
    const textValue = String(detalle.resultado_texto ?? '').trim();
    if (this.lubricantMetricUsesTextResult(definition)) {
      if (!textValue) return 'N/D';
      if (String(definition?.key || '') === 'HUMEDAD') {
        const normalized = this.normalizeSearchToken(textValue);
        if (['POSITIVO', 'POSITIVE'].includes(normalized)) return 'ANORMAL';
        if (['NEGATIVO', 'NEGATIVE'].includes(normalized)) return 'NORMAL';
        return 'N/D';
      }
      if (String(definition?.key || '') === 'COMBUSTIBLE') {
        const normalized = this.normalizeSearchToken(textValue);
        if (['POSITIVO', 'POSITIVE', 'PRESENTE', 'PRESENCE'].includes(normalized))
          return 'ANORMAL';
        if (['NEGATIVO', 'NEGATIVE', 'AUSENTE', 'ABSENT'].includes(normalized))
          return 'NORMAL';
        return 'N/D';
      }
    }
    if (definition) {
      const normalizedText = this.normalizeSearchToken(textValue);
      if (
        normalizedText &&
        definition.textAlert?.some(
          (item) => this.normalizeSearchToken(item) === normalizedText,
        )
      ) {
        return 'ANORMAL';
      }
      if (
        normalizedText &&
        definition.textWarning?.some(
          (item) => this.normalizeSearchToken(item) === normalizedText,
        )
      ) {
        return 'PRECAUCION';
      }
    }

    const currentValue =
      detalle.resultado_numerico == null
        ? null
        : Number(detalle.resultado_numerico);
    if (currentValue == null || !Number.isFinite(currentValue)) {
      return 'N/D';
    }

    if (
      definition?.numericAlertMin != null &&
      currentValue >= definition.numericAlertMin
    ) {
      return 'ANORMAL';
    }
    if (
      definition?.numericWarningMin != null &&
      currentValue >= definition.numericWarningMin
    ) {
      return 'PRECAUCION';
    }

    const baseline =
      baselineValue == null ? null : Number(baselineValue);
    if (baseline != null && Number.isFinite(baseline) && baseline > 0) {
      if (
        definition?.lowerAlertMultiplier != null &&
        currentValue <= baseline * definition.lowerAlertMultiplier
      ) {
        return 'ANORMAL';
      }
      if (
        definition?.upperAlertMultiplier != null &&
        currentValue >= baseline * definition.upperAlertMultiplier
      ) {
        return 'ANORMAL';
      }
      if (
        definition?.lowerWarnMultiplier != null &&
        currentValue <= baseline * definition.lowerWarnMultiplier
      ) {
        return 'PRECAUCION';
      }
      if (
        definition?.upperWarnMultiplier != null &&
        currentValue >= baseline * definition.upperWarnMultiplier
      ) {
        return 'PRECAUCION';
      }

      if (currentValue >= baseline * 2) return 'ANORMAL';
      if (currentValue >= baseline * 1.25) return 'PRECAUCION';
    }

    return 'NORMAL';
  }

  private inferAnalisisStateFromDetails(
    detalles: Partial<AnalisisLubricanteDetalleEntity>[],
    fallback?: string | null,
  ) {
    const levels = detalles.map((item) =>
      this.normalizeLubricantDetailLevel(item.nivel_alerta),
    );
    if (levels.includes('ANORMAL')) return 'ANORMAL';
    if (levels.includes('PRECAUCION')) return 'PRECAUCION';
    const fallbackProvided = String(fallback ?? '').trim() !== '';
    if (fallbackProvided) {
      return this.normalizeLubricantCondition(fallback);
    }
    if (levels.some((item) => item === 'NORMAL')) return 'NORMAL';
    return 'N/D';
  }

  private async buildPreviousLubricantDetailMap(options: {
    analisisId?: string | null;
    lubricante?: string | null;
    compartimento?: string | null;
  }) {
    const normalizedLubricante = this.normalizeSearchToken(options.lubricante);
    const normalizedCompartimento = this.normalizeSearchToken(
      options.compartimento,
    );
    if (!normalizedLubricante) {
      return new Map<string, AnalisisLubricanteDetalleEntity>();
    }

    const rows = await this.analisisLubricanteRepo.find({
      where: { is_deleted: false },
      order: { fecha_reporte: 'DESC', fecha_muestra: 'DESC', created_at: 'DESC' },
      take: 250,
    });

    const previous = rows.find((item) => {
      if (options.analisisId && item.id === options.analisisId) return false;
      const identity = this.resolveLubricantIdentity(item);
      if (identity.lubricante_lookup_key !== normalizedLubricante) return false;
      if (normalizedCompartimento) {
        const compartimento = this.normalizeSearchToken(
          item.compartimento_principal,
        );
        if (compartimento !== normalizedCompartimento) return false;
      }
      return true;
    });

    if (!previous) {
      return new Map<string, AnalisisLubricanteDetalleEntity>();
    }

    const details = await this.analisisLubricanteDetRepo.find({
      where: { analisis_id: previous.id, is_deleted: false },
      order: { orden: 'ASC', created_at: 'ASC' },
    });

    return new Map(
      details.map((item) => [
        this.getLubricantMetricDefinition(item.parametro)?.key ||
          this.normalizeSearchToken(item.parametro),
        item,
      ]),
    );
  }

  private async prepareAnalisisDetallesForSave(options: {
    analisisId?: string | null;
    lubricante?: string | null;
    compartimento?: string | null;
    detalles?: CreateAnalisisLubricanteDto['detalles'];
  }) {
    if (!options.detalles?.length) return [];

    const previousMap = await this.buildPreviousLubricantDetailMap(options);

    return options.detalles.map((detalle, index) => {
      const definition = this.getLubricantMetricDefinition(detalle.parametro);
      const previous =
        previousMap.get(
          definition?.key || this.normalizeSearchToken(detalle.parametro),
        ) ?? null;
      const rawText = String(detalle.resultado_texto ?? '').trim();
      let normalizedText = rawText || null;

      if (['HUMEDAD', 'COMBUSTIBLE'].includes(String(definition?.key || ''))) {
        const humidityToken = this.normalizeSearchToken(rawText);
        if (rawText && !['NEGATIVO', 'POSITIVO'].includes(humidityToken)) {
          throw new BadRequestException(
            `El parametro ${definition?.label || detalle.parametro} solo permite NEGATIVO o POSITIVO.`,
          );
        }
        normalizedText = rawText ? humidityToken : normalizedText;
      } else if (normalizedText) {
        normalizedText = normalizedText.toUpperCase();
      }

      const baselineValue =
        detalle.linea_base ??
        previous?.linea_base ??
        previous?.resultado_numerico ??
        null;
      const currentNumeric =
        this.lubricantMetricUsesTextResult(definition)
          ? null
          : detalle.resultado_numerico == null
          ? null
          : Number(detalle.resultado_numerico);
      const previousNumeric =
        previous?.resultado_numerico == null
          ? null
          : Number(previous.resultado_numerico);

      const trendValue =
        detalle.tendencia != null
          ? this.toNumeric(detalle.tendencia, 0)
          : currentNumeric != null &&
            Number.isFinite(currentNumeric) &&
            previousNumeric != null &&
            Number.isFinite(previousNumeric)
          ? Number((currentNumeric - previousNumeric).toFixed(4))
          : null;

      return {
        ...detalle,
        compartimento:
          detalle.compartimento || options.compartimento || 'GENERAL',
        parametro: definition?.label || detalle.parametro,
        unidad: detalle.unidad ?? previous?.unidad ?? definition?.unit ?? null,
        resultado_numerico:
          currentNumeric != null && Number.isFinite(currentNumeric)
            ? this.toNumeric(currentNumeric, 0)
            : null,
        resultado_texto: normalizedText,
        linea_base:
          baselineValue == null || !Number.isFinite(Number(baselineValue))
            ? null
            : this.toNumeric(baselineValue, 0),
        tendencia: trendValue,
        nivel_alerta: this.evaluateLubricantDetailLevel(
          {
            ...detalle,
            resultado_numerico: currentNumeric,
            resultado_texto: normalizedText,
          },
          baselineValue,
        ),
        orden: detalle.orden ?? definition?.order ?? previous?.orden ?? index + 1,
      };
    });
  }

  private async buildAnalisisLubricantePayload(row: AnalisisLubricanteEntity) {
    const detalles = await this.analisisLubricanteDetRepo.find({
      where: { analisis_id: row.id, is_deleted: false },
      order: { orden: 'ASC', created_at: 'ASC' },
    });
    const payload = (row.payload_json ?? {}) as Record<string, unknown>;
    const identity = this.resolveLubricantIdentity(row);
    const sampleInfo = this.extractAnalysisSampleInfo(payload);
    const enrichedDetails = detalles.map((detalle) => {
      const definition = this.getLubricantMetricDefinition(detalle.parametro);
      const nivel = this.evaluateLubricantDetailLevel(
        detalle,
        detalle.linea_base,
      );
      const baseline =
        detalle.linea_base == null ? null : Number(detalle.linea_base);
      const current =
        detalle.resultado_numerico == null
          ? null
          : Number(detalle.resultado_numerico);
      const trend =
        detalle.tendencia == null ? null : Number(detalle.tendencia);
      const deltaPercent =
        baseline != null &&
        Number.isFinite(baseline) &&
        baseline !== 0 &&
        current != null &&
        Number.isFinite(current)
          ? Number((((current - baseline) / baseline) * 100).toFixed(2))
          : null;

      return {
        ...detalle,
        parametro_label: definition?.label || detalle.parametro,
        parametro_key:
          definition?.key || this.normalizeSearchToken(detalle.parametro),
        grupo: definition?.group ?? 'OTROS_ELEMENTOS',
        grupo_label:
          LUBRICANT_GROUP_LABELS[definition?.group ?? 'OTROS_ELEMENTOS'],
        chart_group: definition?.chartGroup ?? 'otros',
        nivel_alerta: nivel,
        linea_base_resuelta:
          baseline != null && Number.isFinite(baseline) ? baseline : null,
        delta_valor:
          trend != null && Number.isFinite(trend) ? Number(trend.toFixed(4)) : null,
        delta_porcentaje: deltaPercent,
      };
    });

    const groupedDetails = Object.values(
      enrichedDetails.reduce<
        Record<
          string,
          {
            key: string;
            label: string;
            order: number;
            detalles: typeof enrichedDetails;
          }
        >
      >((acc, detalle) => {
        const key = String(detalle.grupo || 'OTROS_ELEMENTOS');
        if (!acc[key]) {
          acc[key] = {
            key,
            label: detalle.grupo_label,
            order:
              LUBRICANT_METRIC_DEFINITIONS.find((item) => item.group === key)
                ?.order ?? 999,
            detalles: [],
          };
        }
        acc[key].detalles.push(detalle);
        return acc;
      }, {}),
    )
      .map((item) => ({
        ...item,
        detalles: item.detalles.sort(
          (a, b) =>
            Number(a.orden ?? 999) - Number(b.orden ?? 999) ||
            String(a.parametro_label ?? '').localeCompare(
              String(b.parametro_label ?? ''),
            ),
        ),
      }))
      .sort((a, b) => a.order - b.order);

    const resolvedCondition = this.inferAnalisisStateFromDetails(
      enrichedDetails,
      row.estado_diagnostico || sampleInfo.condicion,
    );
    const generatedDiagnostic = this.buildLubricantDiagnosticText(
      enrichedDetails,
      resolvedCondition,
    );
    const totals = enrichedDetails.reduce(
      (acc, detalle) => {
        const level = this.normalizeLubricantDetailLevel(detalle.nivel_alerta);
        if (level === 'ANORMAL') acc.anormal += 1;
        else if (level === 'PRECAUCION') acc.precaucion += 1;
        else if (level === 'N/D') acc.nd += 1;
        else acc.normal += 1;
        return acc;
      },
      { anormal: 0, precaucion: 0, normal: 0, nd: 0 },
    );

    return {
      ...row,
      lubricante: identity.lubricante,
      marca_lubricante: identity.marca_lubricante,
      lubricante_codigo: identity.lubricante_codigo,
      lubricante_label: identity.lubricante_label || identity.lubricante,
      sample_info: sampleInfo,
      estado_diagnostico: resolvedCondition,
      evaluacion_ultima_muestra: this.buildLubricantEvaluationLabel(
        resolvedCondition,
      ),
      diagnostico: String(row.diagnostico || '').trim() || generatedDiagnostic,
      detalles: enrichedDetails,
      detalle_grupos: groupedDetails,
      resumen_detalles: {
        total: enrichedDetails.length,
        ...totals,
      },
    };
  }

  private resolveAnalisisFechaReferencia(
    row: Partial<AnalisisLubricanteEntity> & { created_at?: Date | string | null },
  ) {
    return (
      this.safeDateOnlyString(row.fecha_reporte) ??
      this.safeDateOnlyString(row.fecha_muestra) ??
      this.safeDateOnlyString(row.created_at) ??
      null
    );
  }

  private buildAnalisisImportIdentity(input: {
    equipo_id?: string | null;
    equipo_codigo?: string | null;
    compartimento_principal?: string | null;
    fecha_muestra?: string | null;
    lubricante?: string | null;
    payload_json?: Record<string, unknown> | null;
  }) {
    const payload = (input.payload_json ?? {}) as Record<string, unknown>;
    const sampleInfo = this.extractAnalysisSampleInfo(payload);
    const numeroMuestra = String(sampleInfo.numero_muestra ?? '').trim();
    const fechaMuestra =
      this.safeDateOnlyString(input.fecha_muestra) ??
      this.safeDateOnlyString(payload.fecha_muestra) ??
      null;
    const compartimento = this.normalizeSearchToken(input.compartimento_principal);
    const equipoKey =
      String(input.equipo_id || '').trim() ||
      this.normalizeSearchToken(input.equipo_codigo) ||
      'GLOBAL';
    const lubricanteKey = this.normalizeSearchToken(input.lubricante);

    if (!numeroMuestra && !fechaMuestra) {
      return null;
    }

    return [
      equipoKey,
      compartimento || 'GENERAL',
      numeroMuestra || 'SIN_MUESTRA',
      fechaMuestra || 'SIN_FECHA',
      lubricanteKey || 'SIN_LUBRICANTE',
    ].join('::');
  }

  private coerceBoolean(value: unknown, fallback = false) {
    if (typeof value === 'boolean') return value;
    const raw = String(value ?? '').trim().toLowerCase();
    if (!raw) return fallback;
    if (['true', '1', 'si', 'sí', 'yes'].includes(raw)) return true;
    if (['false', '0', 'no'].includes(raw)) return false;
    return fallback;
  }

  private normalizeWorkbookToken(value: unknown) {
    return String(value ?? '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/\s+/g, ' ')
      .trim()
      .toUpperCase();
  }

  private slugifyWorkbookToken(value: unknown) {
    return this.normalizeWorkbookToken(value).replace(/[^A-Z0-9]/g, '');
  }

  private getWorkbookCellValue(
    sheet: XLSX.WorkSheet | undefined,
    rowNumber: number,
    columnNumber: number,
  ) {
    if (!sheet) return null;
    const cell = sheet[
      XLSX.utils.encode_cell({ r: rowNumber - 1, c: columnNumber - 1 })
    ];
    return cell?.v ?? null;
  }

  private getWorkbookCellText(
    sheet: XLSX.WorkSheet | undefined,
    rowNumber: number,
    columnNumber: number,
  ) {
    const value = this.getWorkbookCellValue(sheet, rowNumber, columnNumber);
    return String(value ?? '').trim();
  }

  private getWorkbookCellNumber(
    sheet: XLSX.WorkSheet | undefined,
    rowNumber: number,
    columnNumber: number,
  ) {
    const value = this.getWorkbookCellValue(sheet, rowNumber, columnNumber);
    if (value == null || value === '') return null;
    const parsed =
      typeof value === 'number'
        ? value
        : Number(String(value).replace(/,/g, '.'));
    return Number.isFinite(parsed) ? parsed : null;
  }

  private getWorkbookCellDate(
    sheet: XLSX.WorkSheet | undefined,
    rowNumber: number,
    columnNumber: number,
  ) {
    const value = this.getWorkbookCellValue(sheet, rowNumber, columnNumber);
    return this.safeDateOnlyString(value);
  }

  private normalizeImportedCompartment(value: unknown) {
    const normalized = this.normalizeWorkbookToken(value);
    return (
      LUBRICANT_IMPORT_SHEETS.find(
        (item) => this.normalizeWorkbookToken(item) === normalized,
      ) ?? String(value ?? '').trim().toUpperCase()
    );
  }

  private resolveWorkbookEquipmentHint(
    workbook: XLSX.WorkBook,
    fileName: string,
  ) {
    const motorSheet =
      workbook.Sheets.MOTOR ||
      workbook.Sheets[
        workbook.SheetNames.find(
          (name) => this.normalizeWorkbookToken(name) === 'MOTOR',
        ) || ''
      ];
    const byHeader = this.getWorkbookCellText(motorSheet, 3, 20);
    if (byHeader) return byHeader;

    const dataSheet = workbook.Sheets.Datos;
    const byDatos = this.getWorkbookCellText(dataSheet, 28, 9);
    if (byDatos) return byDatos;

    const match = String(fileName || '')
      .toUpperCase()
      .match(/UGN?\s*[- ]?\s*0*(\d{1,3})/);
    if (match) {
      const [, rawCode = ''] = match;
      return `UG ${rawCode.padStart(2, '0')}`;
    }

    return '';
  }

  private async resolveLubricantImportEquipment(hint: string) {
    const normalizedHint = this.slugifyWorkbookToken(hint);
    const numericHint = normalizedHint.replace(/[^0-9]/g, '');
    if (!normalizedHint && !numericHint) {
      return {
        equipo: null as EquipoEntity | null,
        marcaNombre: null as string | null,
        equipoCodigo: '',
        equipoNombre: '',
      };
    }

    const equipments = await this.equipoRepo.find({
      where: { is_deleted: false },
    });
    const brandIds = [
      ...new Set(
        equipments
          .map((item) => String(item.marca_id ?? '').trim())
          .filter(Boolean),
      ),
    ];
    const brands = brandIds.length
      ? await this.marcaRepo.find({ where: { id: In(brandIds), is_deleted: false } })
      : [];
    const brandMap = new Map(
      brands.map((item) => [String(item.id), String(item.nombre ?? '').trim()]),
    );

    const match = equipments.find((item) => {
      const code = this.slugifyWorkbookToken(item.codigo);
      const name = this.slugifyWorkbookToken(item.nombre);
      const numericCode = code.replace(/[^0-9]/g, '');
      return (
        (normalizedHint && (code === normalizedHint || name === normalizedHint)) ||
        (numericHint &&
          numericCode === numericHint &&
          numericHint.length === numericCode.length) ||
        (normalizedHint &&
          ((code && normalizedHint.includes(code)) ||
            (name && normalizedHint.includes(name)) ||
            (code && code.includes(normalizedHint)) ||
            (name && name.includes(normalizedHint))))
      );
    });

    return {
      equipo: match ?? null,
      marcaNombre: match
        ? String(brandMap.get(String(match.marca_id ?? '')) ?? '').trim() || null
        : null,
      equipoCodigo: String(match?.codigo ?? hint ?? '').trim(),
      equipoNombre: String(match?.nombre ?? '').trim(),
    };
  }

  private isMeaningfulWorkbookSampleId(value: unknown) {
    const normalized = String(value ?? '').trim().toUpperCase();
    if (!normalized) return false;
    return !['0', '0.0', 'N/D', 'ND', 'NULL', 'NONE'].includes(normalized);
  }

  private hasLubricantSampleData(
    sheet: XLSX.WorkSheet | undefined,
    headerRow: number,
    columnNumber: number,
  ) {
    const numeroMuestra = this.getWorkbookCellValue(
      sheet,
      headerRow + 10,
      columnNumber,
    );
    if (this.isMeaningfulWorkbookSampleId(numeroMuestra)) return true;

    if (this.getWorkbookCellDate(sheet, headerRow + 11, columnNumber)) return true;
    if (this.getWorkbookCellDate(sheet, headerRow + 12, columnNumber)) return true;
    if (this.getWorkbookCellDate(sheet, headerRow + 13, columnNumber)) return true;
    if (this.getWorkbookCellNumber(sheet, headerRow + 14, columnNumber) != null)
      return true;
    if (this.getWorkbookCellNumber(sheet, headerRow + 15, columnNumber) != null)
      return true;

    const condicion = this.getWorkbookCellText(sheet, headerRow + 16, columnNumber);
    if (condicion) return true;

    const parameterRowOffset = headerRow - 2;
    return LUBRICANT_IMPORT_PARAMETER_ROWS.some((item) => {
      const value = this.getWorkbookCellValue(
        sheet,
        item.row + parameterRowOffset,
        columnNumber,
      );
      return value != null && String(value).trim() !== '';
    });
  }

  private getLubricantSheetHeaderRows(sheet: XLSX.WorkSheet | undefined) {
    if (!sheet) return [];
    const range = XLSX.utils.decode_range(sheet['!ref'] || 'A1:A1');
    const starts: number[] = [];

    for (let row = 1; row <= range.e.r + 1; row += 1) {
      const compartimentoLabel = this.slugifyWorkbookToken(
        this.getWorkbookCellText(sheet, row, 8),
      );
      const equipoLabel = this.slugifyWorkbookToken(
        this.getWorkbookCellText(sheet, row + 1, 8),
      );
      const lubricanteLabel = this.slugifyWorkbookToken(
        this.getWorkbookCellText(sheet, row + 5, 8),
      );
      const marcaLubricanteLabel = this.slugifyWorkbookToken(
        this.getWorkbookCellText(sheet, row + 6, 8),
      );

      if (
        compartimentoLabel === 'COMPARTIMENTO' &&
        equipoLabel === 'EQUIPO' &&
        lubricanteLabel === 'LUBRICANTE' &&
        marcaLubricanteLabel === 'MARCADELLUBRICANTE'
      ) {
        starts.push(row);
      }
    }

    return [...new Set(starts)];
  }

  private parseLubricantWorkbookLegacy(
    buffer: Buffer,
    fileName: string,
  ): Promise<ParsedLubricantWorkbook> {
    return (async () => {
      const workbook = XLSX.read(buffer, {
        type: 'buffer',
        cellDates: true,
      });
      const warnings: string[] = [];
      const validSheets = workbook.SheetNames.filter((sheetName) =>
        LUBRICANT_IMPORT_SHEETS.some(
          (item) =>
            this.normalizeWorkbookToken(item) ===
            this.normalizeWorkbookToken(sheetName),
        ),
      );

      if (!validSheets.length) {
        throw new BadRequestException(
          'El archivo no contiene hojas válidas de análisis de lubricante.',
        );
      }

      const workbookEquipmentHint = this.resolveWorkbookEquipmentHint(
        workbook,
        fileName,
      );
      const workbookEquipment = await this.resolveLubricantImportEquipment(
        workbookEquipmentHint,
      );
      const analyses: CreateAnalisisLubricanteDto[] = [];

      for (const sheetName of validSheets) {
        const sheet = workbook.Sheets[sheetName];
        if (!sheet) continue;
        const compartimento = this.normalizeImportedCompartment(
          this.getWorkbookCellText(sheet, 2, 20) || sheetName,
        );
        const equipmentHint =
          this.getWorkbookCellText(sheet, 3, 20) || workbookEquipmentHint;
        const equipmentContext = equipmentHint
          ? await this.resolveLubricantImportEquipment(equipmentHint)
          : workbookEquipment;
        const cliente = this.getWorkbookCellText(sheet, 4, 3) || 'JUSTICE COMPANY';
        const lubricante = this.getWorkbookCellText(sheet, 7, 20);
        const marcaLubricante = this.getWorkbookCellText(sheet, 8, 20);
        const serie = this.getWorkbookCellText(sheet, 5, 20) || null;
        const modelo = this.getWorkbookCellText(sheet, 6, 20) || null;
        const marcaEquipo =
          equipmentContext.marcaNombre ||
          this.getWorkbookCellText(sheet, 4, 20) ||
          null;

        if (!lubricante) {
          warnings.push(
            `La hoja ${sheetName} no contiene valor en la posición T7 (Lubricante).`,
          );
        }
        if (!marcaLubricante) {
          warnings.push(
            `La hoja ${sheetName} no contiene valor en la posición T8 (Marca del Lubricante).`,
          );
        }

        const sampleColumns: number[] = [];
        const range = XLSX.utils.decode_range(sheet['!ref'] || 'A1:A1');
        for (let column = 3; column <= range.e.c + 1; column += 1) {
          const numeroMuestra = this.getWorkbookCellText(sheet, 12, column);
          if (numeroMuestra) sampleColumns.push(column);
        }
        if (!sampleColumns.length) {
          warnings.push(
            `La hoja ${sheetName} no contiene columnas de muestra en la fila 12.`,
          );
          continue;
        }

        for (const column of sampleColumns) {
          const numeroMuestra = this.getWorkbookCellText(sheet, 12, column);
          const fechaMuestra = this.getWorkbookCellDate(sheet, 13, column);
          const fechaIngreso = this.getWorkbookCellDate(sheet, 14, column);
          const fechaInforme = this.getWorkbookCellDate(sheet, 15, column);
          const horasEquipo = this.getWorkbookCellNumber(sheet, 16, column);
          const horasLubricante = this.getWorkbookCellNumber(sheet, 17, column);
          const condicion = this.normalizeLubricantCondition(
            this.getWorkbookCellText(sheet, 18, column) || 'N/D',
          );

          if (!numeroMuestra && !fechaMuestra && !fechaInforme) {
            continue;
          }

          const detalles = LUBRICANT_IMPORT_PARAMETER_ROWS.map((item) => {
            const definition = this.getLubricantMetricDefinition(item.label);
            const textValue = this.getWorkbookCellText(sheet, item.row, column);
            const numericValue = this.getWorkbookCellNumber(sheet, item.row, column);
            const usesText = this.lubricantMetricUsesTextResult(definition);
            const normalizedText =
              ['HUMEDAD', 'COMBUSTIBLE'].includes(
                String(definition?.key || ''),
              )
                ? this.normalizeSearchToken(textValue)
                : textValue || null;

            return {
              compartimento,
              numero_muestra: numeroMuestra || undefined,
              parametro: definition?.label || item.label,
              resultado_numerico:
                usesText || normalizedText
                  ? usesText
                    ? undefined
                    : numericValue ?? undefined
                  : numericValue ?? undefined,
              resultado_texto:
                usesText || (!Number.isFinite(Number(numericValue)) && normalizedText)
                  ? normalizedText ?? undefined
                  : undefined,
              unidad: definition?.unit ?? undefined,
              orden: definition?.order ?? item.row,
            };
          });

          analyses.push({
            cliente,
            equipo_id: equipmentContext.equipo?.id ?? undefined,
            equipo_codigo: equipmentContext.equipoCodigo || undefined,
            equipo_nombre: equipmentContext.equipoNombre || undefined,
            lubricante: lubricante || undefined,
            marca_lubricante: marcaLubricante || undefined,
            compartimento_principal: compartimento || undefined,
            fecha_muestra: fechaMuestra || undefined,
            fecha_reporte: fechaInforme || undefined,
            estado_diagnostico: condicion,
            documento_origen: fileName,
            payload_json: {
              sample_info: {
                numero_muestra: numeroMuestra || undefined,
                fecha_ingreso: fechaIngreso,
                fecha_informe: fechaInforme,
                horas_equipo: horasEquipo,
                horas_lubricante: horasLubricante,
                condicion,
                equipo_marca: marcaEquipo,
                equipo_serie: serie,
                equipo_modelo: modelo,
                lubricante: lubricante || null,
                marca_lubricante: marcaLubricante || null,
                compartimento,
                hoja_origen: sheetName,
              },
            },
            detalles,
          });
        }
      }

      return {
        analyses,
        warnings,
        sheets: validSheets,
      };
    })();
  }

  private parseLubricantWorkbook(
    buffer: Buffer,
    fileName: string,
  ): Promise<ParsedLubricantWorkbook> {
    return (async () => {
      const workbook = XLSX.read(buffer, {
        type: 'buffer',
        cellDates: true,
      });
      const warnings: string[] = [];
      const validSheets = workbook.SheetNames.filter((sheetName) =>
        LUBRICANT_IMPORT_SHEETS.some(
          (item) =>
            this.normalizeWorkbookToken(item) ===
            this.normalizeWorkbookToken(sheetName),
        ),
      );

      if (!validSheets.length) {
        throw new BadRequestException(
          'El archivo no contiene hojas válidas de análisis de lubricante.',
        );
      }

      const workbookEquipmentHint = this.resolveWorkbookEquipmentHint(
        workbook,
        fileName,
      );
      const workbookEquipment = await this.resolveLubricantImportEquipment(
        workbookEquipmentHint,
      );
      const analyses: CreateAnalisisLubricanteDto[] = [];

      for (const sheetName of validSheets) {
        const sheet = workbook.Sheets[sheetName];
        if (!sheet) continue;

        const detectedHeaderRows = this.getLubricantSheetHeaderRows(sheet);
        const headerRows = detectedHeaderRows.length ? detectedHeaderRows : [2];

        if (detectedHeaderRows.length > 1) {
          warnings.push(
            `La hoja ${sheetName} contiene ${detectedHeaderRows.length} cabeceras de análisis. Se procesarán como bloques independientes.`,
          );
        }

        for (let blockIndex = 0; blockIndex < headerRows.length; blockIndex += 1) {
          const headerRow = headerRows[blockIndex];
          const rowOffset = headerRow - 2;
          const compartimento = this.normalizeImportedCompartment(
            this.getWorkbookCellText(sheet, headerRow, 20) || sheetName,
          );
          const equipmentHint =
            this.getWorkbookCellText(sheet, headerRow + 1, 20) ||
            workbookEquipmentHint;
          const equipmentContext = equipmentHint
            ? await this.resolveLubricantImportEquipment(equipmentHint)
            : workbookEquipment;
          const cliente =
            this.getWorkbookCellText(sheet, headerRow + 2, 3) ||
            'JUSTICE COMPANY';
          const lubricante = this.getWorkbookCellText(sheet, headerRow + 5, 20);
          const marcaLubricante = this.getWorkbookCellText(
            sheet,
            headerRow + 6,
            20,
          );
          const serie =
            this.getWorkbookCellText(sheet, headerRow + 3, 20) || null;
          const modelo =
            this.getWorkbookCellText(sheet, headerRow + 4, 20) || null;
          const marcaEquipo =
            equipmentContext.marcaNombre ||
            this.getWorkbookCellText(sheet, headerRow + 2, 20) ||
            null;

          const range = XLSX.utils.decode_range(sheet['!ref'] || 'A1:A1');
          const sampleColumns: number[] = [];
          for (let column = 3; column <= range.e.c + 1; column += 1) {
            if (this.hasLubricantSampleData(sheet, headerRow, column)) {
              sampleColumns.push(column);
            }
          }

          if (
            !lubricante &&
            !marcaLubricante &&
            !equipmentHint &&
            !sampleColumns.length
          ) {
            continue;
          }

          const blockLabel =
            headerRows.length > 1 ? ` (bloque ${blockIndex + 1})` : '';

          if (!lubricante) {
            warnings.push(
              `La hoja ${sheetName}${blockLabel} no contiene valor en la cabecera de Lubricante.`,
            );
          }
          if (!marcaLubricante) {
            warnings.push(
              `La hoja ${sheetName}${blockLabel} no contiene valor en la cabecera de Marca del Lubricante.`,
            );
          }
          if (!equipmentHint) {
            warnings.push(
              `La hoja ${sheetName}${blockLabel} no contiene valor en la cabecera de Equipo.`,
            );
          }
          if (!sampleColumns.length) {
            warnings.push(
              `La hoja ${sheetName}${blockLabel} no contiene muestras válidas debajo de la cabecera.`,
            );
          }

          if (!lubricante || !marcaLubricante || !sampleColumns.length) {
            continue;
          }

          for (const column of sampleColumns) {
            const numeroMuestraRaw = this.getWorkbookCellValue(
              sheet,
              headerRow + 10,
              column,
            );
            const numeroMuestra = this.isMeaningfulWorkbookSampleId(numeroMuestraRaw)
              ? String(numeroMuestraRaw ?? '').trim()
              : '';
            const fechaMuestra = this.getWorkbookCellDate(
              sheet,
              headerRow + 11,
              column,
            );
            const fechaIngreso = this.getWorkbookCellDate(
              sheet,
              headerRow + 12,
              column,
            );
            const fechaInforme = this.getWorkbookCellDate(
              sheet,
              headerRow + 13,
              column,
            );
            const horasEquipo = this.getWorkbookCellNumber(
              sheet,
              headerRow + 14,
              column,
            );
            const horasLubricante = this.getWorkbookCellNumber(
              sheet,
              headerRow + 15,
              column,
            );
            const condicion = this.normalizeLubricantCondition(
              this.getWorkbookCellText(sheet, headerRow + 16, column) || 'N/D',
            );

            if (
              !fechaMuestra &&
              !fechaIngreso &&
              !fechaInforme
            ) {
              continue;
            }

            const detalles = LUBRICANT_IMPORT_PARAMETER_ROWS.map((item) => {
              const definition = this.getLubricantMetricDefinition(item.label);
              const targetRow = item.row + rowOffset;
              const textValue = this.getWorkbookCellText(
                sheet,
                targetRow,
                column,
              );
              const numericValue = this.getWorkbookCellNumber(
                sheet,
                targetRow,
                column,
              );
              const usesText = this.lubricantMetricUsesTextResult(definition);
              const normalizedText =
                ['HUMEDAD', 'COMBUSTIBLE'].includes(
                  String(definition?.key || ''),
                )
                  ? this.normalizeSearchToken(textValue)
                  : textValue || null;

              const hasTextValue = !!normalizedText;
              const hasNumericValue =
                numericValue != null && Number.isFinite(Number(numericValue));
              if (!hasTextValue && !hasNumericValue) {
                return null;
              }

              return {
                compartimento,
                numero_muestra: numeroMuestra || undefined,
                parametro: definition?.label || item.label,
                resultado_numerico:
                  usesText || hasTextValue
                    ? usesText
                      ? undefined
                      : numericValue ?? undefined
                    : numericValue ?? undefined,
                resultado_texto:
                  usesText || (!hasNumericValue && hasTextValue)
                    ? normalizedText ?? undefined
                    : undefined,
                unidad: definition?.unit ?? undefined,
                orden: definition?.order ?? targetRow,
              };
            }).filter(Boolean) as NonNullable<CreateAnalisisLubricanteDto['detalles']>;

            if (!detalles.length) {
              continue;
            }

            analyses.push({
              cliente,
              equipo_id: equipmentContext.equipo?.id ?? undefined,
              equipo_codigo: equipmentContext.equipoCodigo || undefined,
              equipo_nombre: equipmentContext.equipoNombre || undefined,
              lubricante: lubricante || undefined,
              marca_lubricante: marcaLubricante || undefined,
              compartimento_principal: compartimento || undefined,
              fecha_muestra: fechaMuestra || undefined,
              fecha_reporte: fechaInforme || undefined,
              estado_diagnostico: condicion,
              documento_origen: fileName,
              payload_json: {
                sample_info: {
                  numero_muestra: numeroMuestra || undefined,
                  fecha_ingreso: fechaIngreso,
                  fecha_informe: fechaInforme,
                  horas_equipo: horasEquipo,
                  horas_lubricante: horasLubricante,
                  condicion,
                  equipo_marca: marcaEquipo,
                  equipo_serie: serie,
                  equipo_modelo: modelo,
                  lubricante: lubricante || null,
                  marca_lubricante: marcaLubricante || null,
                  compartimento,
                  hoja_origen: sheetName,
                  bloque_hoja: blockIndex + 1,
                },
              },
              detalles,
            });
          }
        }
      }

      return {
        analyses,
        warnings,
        sheets: validSheets,
      };
    })();
  }

  private resolveDashboardDateRange(
    periodo?: string | null,
    from?: string | null,
    to?: string | null,
  ) {
    const normalizedPeriod = String(periodo || '')
      .trim()
      .toUpperCase();
    const today = new Date();
    let resolvedFrom = this.safeDateOnlyString(from);
    let resolvedTo = this.safeDateOnlyString(to) ?? today.toISOString().slice(0, 10);

    if (!resolvedFrom) {
      const base = new Date(today);
      if (normalizedPeriod === 'SEMANAL') {
        base.setDate(base.getDate() - 7);
        resolvedFrom = base.toISOString().slice(0, 10);
      } else if (normalizedPeriod === 'MENSUAL') {
        base.setMonth(base.getMonth() - 1);
        resolvedFrom = base.toISOString().slice(0, 10);
      } else if (normalizedPeriod === 'ANUAL') {
        base.setFullYear(base.getFullYear() - 1);
        resolvedFrom = base.toISOString().slice(0, 10);
      }
    }

    return {
      periodo:
        normalizedPeriod || (resolvedFrom || resolvedTo ? 'PERSONALIZADO' : 'GLOBAL'),
      from: resolvedFrom,
      to: resolvedTo,
    };
  }

  private async buildCronogramaSemanalPayload(row: CronogramaSemanalEntity) {
    const detalles = await this.cronogramaSemanalDetRepo.find({
      where: { cronograma_id: row.id, is_deleted: false },
      order: { orden: 'ASC', created_at: 'ASC' },
    });
    return {
      ...row,
      detalles,
    };
  }

  private async buildReporteDiarioPayload(row: ReporteOperacionDiariaEntity) {
    const [unidades, combustibles, componentes] = await Promise.all([
      this.reporteDiarioUnidadRepo.find({
        where: { reporte_id: row.id, is_deleted: false },
        order: { created_at: 'ASC' },
      }),
      this.reporteCombustibleRepo.find({
        where: { reporte_id: row.id, is_deleted: false },
        order: { fecha_lectura: 'DESC', created_at: 'DESC' },
      }),
      this.controlComponenteRepo.find({
        where: { reporte_id: row.id, is_deleted: false },
        order: { created_at: 'DESC' },
      }),
    ]);

    return {
      ...row,
      unidades,
      combustibles,
      componentes,
    };
  }

  private pickLatestRowsByKey<T>(
    rows: T[],
    resolveKey: (row: T) => string,
    resolveTimestamp: (row: T) => number,
  ) {
    const latest = new Map<string, T>();
    for (const row of rows) {
      const key = resolveKey(row);
      if (!key) continue;
      const current = latest.get(key);
      if (!current || resolveTimestamp(row) >= resolveTimestamp(current)) {
        latest.set(key, row);
      }
    }
    return [...latest.values()];
  }

  private async buildProgramacionAlertCandidates(): Promise<AlertCandidate[]> {
    const rows = await this.programacionRepo.find({
      where: { is_deleted: false, activo: true },
    });
    const programaciones = await Promise.all(
      rows.map((row) =>
        this.recalculateProgramacionFields(row, { persist: false }),
      ),
    );

    return programaciones.flatMap((row: any) => {
      const estado = String(row.estado_programacion || '').toUpperCase();
      if (!['VENCIDA', 'PROXIMA'].includes(estado)) return [];

      const hoursRemaining =
        row.horas_restantes == null ? null : this.toNumeric(row.horas_restantes);
      const daysRemaining =
        row.dias_restantes == null ? null : this.toNumeric(row.dias_restantes);
      const equipoLabel =
        row.equipo_codigo || row.equipo_nombre || row.equipo_id || 'Equipo';
      const planLabel =
        row.procedimiento_nombre ||
        row.plan_nombre ||
        row.plan_codigo ||
        row.plan_id ||
        'Plan';

      const timing: string[] = [];
      if (hoursRemaining != null) {
        timing.push(
          estado === 'VENCIDA'
            ? `atrasada ${Math.abs(hoursRemaining).toFixed(2)} h`
            : `vence en ${hoursRemaining.toFixed(2)} h`,
        );
      }
      if (daysRemaining != null) {
        timing.push(
          estado === 'VENCIDA'
            ? `atrasada ${Math.abs(daysRemaining)} d`
            : `vence en ${daysRemaining} d`,
        );
      }

      return [
        {
          equipo_id: row.equipo_id ?? null,
          tipo_alerta:
            estado === 'VENCIDA'
              ? 'MANTENIMIENTO_VENCIDO'
              : 'MANTENIMIENTO_PROXIMO',
          categoria: 'MANTENIMIENTO' as AlertCategory,
          nivel: estado === 'VENCIDA' ? 'CRITICAL' : 'WARNING',
          origen: 'PROGRAMACION' as AlertOrigin,
          referencia_tipo: 'PROGRAMACION',
          referencia: `PROGRAMACION:${row.id}`,
          detalle: `${equipoLabel} · ${planLabel}${
            timing.length ? ` · ${timing.join(' / ')}` : ''
          }`,
          payload_json: {
            programacion_id: row.id,
            plan_id: row.plan_id,
            plan_codigo: row.plan_codigo ?? null,
            plan_nombre: row.plan_nombre ?? null,
            procedimiento_id: row.procedimiento_id ?? null,
            procedimiento_codigo: row.procedimiento_codigo ?? null,
            procedimiento_nombre: row.procedimiento_nombre ?? null,
            equipo_id: row.equipo_id ?? null,
            equipo_codigo: row.equipo_codigo ?? null,
            equipo_nombre: row.equipo_nombre ?? null,
            estado_programacion: estado,
            horometro_actual: row.horometro_actual ?? null,
            horas_restantes: hoursRemaining,
            dias_restantes: daysRemaining,
            proxima_horas: row.proxima_horas ?? null,
            proxima_fecha: row.proxima_fecha ?? null,
          },
        },
      ];
    });
  }

  private async buildReporteDiarioAlertCandidates(): Promise<AlertCandidate[]> {
    const unidades = await this.reporteDiarioUnidadRepo.find({
      where: { is_deleted: false },
      order: { created_at: 'DESC', id: 'DESC' },
    });
    const latestUnits = this.pickLatestRowsByKey(
      unidades,
      (row) => String(row.equipo_id || row.equipo_codigo || row.id || '').trim(),
      (row) =>
        new Date(row.updated_at ?? row.created_at ?? 0).getTime() ||
        new Date(row.created_at ?? 0).getTime(),
    );

    const reportIds = [
      ...new Set(latestUnits.map((row) => row.reporte_id).filter(Boolean)),
    ] as string[];
    const reportes = reportIds.length
      ? await this.reporteDiarioRepo.find({
          where: { id: In(reportIds), is_deleted: false },
        })
      : [];
    const reportMap = new Map(reportes.map((row) => [row.id, row]));

    return latestUnits.flatMap((row) => {
      const hoursRemaining =
        row.horas_faltantes == null ? null : this.toNumeric(row.horas_faltantes);
      const daysRemaining =
        row.dias_faltantes == null ? null : this.toNumeric(row.dias_faltantes);
      const dueNow =
        (hoursRemaining != null && hoursRemaining <= 0) ||
        (daysRemaining != null && daysRemaining <= 0);
      const dueSoon =
        !dueNow &&
        ((hoursRemaining != null && hoursRemaining <= 50) ||
          (daysRemaining != null && daysRemaining <= 3));

      if (!dueNow && !dueSoon) return [];

      const reporte = row.reporte_id ? reportMap.get(row.reporte_id) : null;
      const equipoLabel = row.equipo_codigo || row.equipo_id || 'Unidad';
      const mpgLabel = row.proximo_mpg ? ` · ${row.proximo_mpg}` : '';
      const timing: string[] = [];
      if (hoursRemaining != null) {
        timing.push(
          dueNow
            ? `horas faltantes ${hoursRemaining.toFixed(2)}`
            : `faltan ${hoursRemaining.toFixed(2)} h`,
        );
      }
      if (daysRemaining != null) {
        timing.push(
          dueNow
            ? `días faltantes ${daysRemaining}`
            : `faltan ${daysRemaining} d`,
        );
      }

      return [
        {
          equipo_id: row.equipo_id ?? null,
          tipo_alerta: dueNow
            ? 'REPORTE_DIARIO_VENCIDO'
            : 'REPORTE_DIARIO_PROXIMO',
          categoria: 'OPERACION' as AlertCategory,
          nivel: dueNow ? 'CRITICAL' : 'WARNING',
          origen: 'REPORTE_DIARIO' as AlertOrigin,
          referencia_tipo: 'REPORTE_DIARIO',
          referencia: `REPORTE_UNIDAD:${row.id}`,
          detalle: `${equipoLabel}${mpgLabel}${
            timing.length ? ` · ${timing.join(' / ')}` : ''
          }`,
          payload_json: {
            reporte_id: row.reporte_id ?? null,
            reporte_codigo: reporte?.codigo ?? null,
            fecha_reporte: reporte?.fecha_reporte ?? null,
            locacion: reporte?.locacion ?? null,
            turno: reporte?.turno ?? null,
            unidad_id: row.id,
            equipo_id: row.equipo_id ?? null,
            equipo_codigo: row.equipo_codigo,
            proximo_mpg: row.proximo_mpg ?? null,
            horometro_actual: row.horometro_actual ?? null,
            horas_faltantes: hoursRemaining,
            dias_faltantes: daysRemaining,
            fecha_proxima: row.fecha_proxima ?? null,
            nota: row.nota ?? null,
          },
        },
      ];
    });
  }

  private async buildLubricanteAlertCandidates(): Promise<AlertCandidate[]> {
    const analisis = await this.analisisLubricanteRepo.find({
      where: { is_deleted: false },
      order: { fecha_reporte: 'DESC', created_at: 'DESC' },
    });
    const latestRows = this.pickLatestRowsByKey(
      analisis,
      (row) =>
        [
          row.equipo_id || row.equipo_codigo || row.codigo,
          row.compartimento_principal || 'GENERAL',
        ]
          .map((value) => String(value || '').trim())
          .join('::'),
      (row) =>
        new Date(row.fecha_reporte || row.created_at || 0).getTime() ||
        new Date(row.created_at || 0).getTime(),
    );

    const analisisIds = latestRows.map((row) => row.id);
    const detalles = analisisIds.length
      ? await this.analisisLubricanteDetRepo.find({
          where: { analisis_id: In(analisisIds), is_deleted: false },
          order: { orden: 'ASC', created_at: 'ASC' },
        })
      : [];
    const detailsMap = new Map<string, AnalisisLubricanteDetalleEntity[]>();
    for (const detalle of detalles) {
      const group = detailsMap.get(detalle.analisis_id) ?? [];
      group.push(detalle);
      detailsMap.set(detalle.analisis_id, group);
    }

    return latestRows.flatMap((row) => {
      const rowDetails = detailsMap.get(row.id) ?? [];
      const abnormalDetails = rowDetails.filter(
        (detalle) => this.normalizeAlertLevel(detalle.nivel_alerta) !== 'INFO',
      );
      const level = this.maxAlertLevel(
        row.estado_diagnostico,
        ...abnormalDetails.map((detalle) => detalle.nivel_alerta),
      );
      if (level === 'INFO') return [];

      const highlights = abnormalDetails
        .slice(0, 3)
        .map((detalle) => {
          const result =
            detalle.resultado_texto ??
            (detalle.resultado_numerico != null
              ? `${detalle.resultado_numerico}${detalle.unidad ? ` ${detalle.unidad}` : ''}`
              : detalle.nivel_alerta);
          return `${detalle.parametro}: ${result}`;
        })
        .join(', ');

      return [
        {
          equipo_id: row.equipo_id ?? null,
          tipo_alerta:
            level === 'CRITICAL' ? 'LUBRICANTE_CRITICO' : 'LUBRICANTE_ALERTA',
          categoria: 'LUBRICANTE' as AlertCategory,
          nivel: level,
          origen: 'ANALISIS_LUBRICANTE' as AlertOrigin,
          referencia_tipo: 'ANALISIS_LUBRICANTE',
          referencia: `ANALISIS:${row.id}`,
          detalle: `${row.equipo_codigo || row.equipo_nombre || row.codigo} · ${
            row.compartimento_principal || 'Compartimento'
          }${
            highlights
              ? ` · ${highlights}`
              : row.diagnostico
                ? ` · ${row.diagnostico}`
                : ''
          }`,
          payload_json: {
            analisis_id: row.id,
            codigo: row.codigo,
            equipo_id: row.equipo_id ?? null,
            equipo_codigo: row.equipo_codigo ?? null,
            equipo_nombre: row.equipo_nombre ?? null,
            compartimento_principal: row.compartimento_principal ?? null,
            fecha_muestra: row.fecha_muestra ?? null,
            fecha_reporte: row.fecha_reporte ?? null,
            estado_diagnostico: row.estado_diagnostico,
            diagnostico: row.diagnostico ?? null,
            documento_origen: row.documento_origen ?? null,
            parametros_alerta: abnormalDetails.map((detalle) => ({
              parametro: detalle.parametro,
              nivel_alerta: detalle.nivel_alerta,
              resultado_numerico: detalle.resultado_numerico ?? null,
              resultado_texto: detalle.resultado_texto ?? null,
              observacion: detalle.observacion ?? null,
            })),
          },
        },
      ];
    });
  }

  private async buildFuelAlertCandidates(): Promise<AlertCandidate[]> {
    const rows = await this.reporteCombustibleRepo.find({
      where: { is_deleted: false },
      order: { fecha_lectura: 'DESC', created_at: 'DESC' },
    });
    const latestRows = this.pickLatestRowsByKey(
      rows,
      (row) => String(row.tanque || row.id || '').trim(),
      (row) =>
        new Date(row.fecha_lectura || row.updated_at || row.created_at || 0).getTime(),
    );

    const reportIds = [
      ...new Set(latestRows.map((row) => row.reporte_id).filter(Boolean)),
    ] as string[];
    const reportes = reportIds.length
      ? await this.reporteDiarioRepo.find({
          where: { id: In(reportIds), is_deleted: false },
        })
      : [];
    const reportMap = new Map(reportes.map((row) => [row.id, row]));

    return latestRows.flatMap((row) => {
      const stockActual =
        row.stock_actual != null
          ? this.toNumeric(row.stock_actual)
          : row.galones != null
            ? this.toNumeric(row.galones)
            : null;
      const stockMinimo =
        row.stock_minimo == null ? null : this.toNumeric(row.stock_minimo);
      if (stockActual == null || stockMinimo == null || stockMinimo <= 0) return [];

      const isCritical = stockActual <= stockMinimo;
      const isWarning = !isCritical && stockActual <= stockMinimo * 1.1;
      if (!isCritical && !isWarning) return [];

      const reporte = row.reporte_id ? reportMap.get(row.reporte_id) : null;
      return [
        {
          equipo_id: null,
          tipo_alerta: isCritical
            ? 'COMBUSTIBLE_BAJO'
            : 'COMBUSTIBLE_PROXIMO_MINIMO',
          categoria: 'COMBUSTIBLE' as AlertCategory,
          nivel: isCritical ? 'CRITICAL' : 'WARNING',
          origen: 'COMBUSTIBLE' as AlertOrigin,
          referencia_tipo: 'COMBUSTIBLE',
          referencia: `COMBUSTIBLE:${row.id}`,
          detalle: `Tanque ${row.tanque} · stock ${stockActual.toFixed(
            2,
          )} gal · mínimo ${stockMinimo.toFixed(2)} gal`,
          payload_json: {
            combustible_id: row.id,
            reporte_id: row.reporte_id ?? null,
            reporte_codigo: reporte?.codigo ?? null,
            fecha_reporte: reporte?.fecha_reporte ?? null,
            tanque: row.tanque,
            fecha_lectura: row.fecha_lectura,
            stock_actual: stockActual,
            stock_minimo: stockMinimo,
            stock_maximo:
              row.stock_maximo == null ? null : this.toNumeric(row.stock_maximo),
            consumo_galones:
              row.consumo_galones == null
                ? null
                : this.toNumeric(row.consumo_galones),
            observacion: row.observacion ?? null,
          },
        },
      ];
    });
  }

  private async buildInventoryAlertCandidates(): Promise<AlertCandidate[]> {
    const rows = await this.stockRepo.find();
    const { productMap, warehouseMap } = await this.buildInventoryCatalogMaps(
      rows.map((row) => row.producto_id),
      rows.map((row) => row.bodega_id),
    );

    return rows.flatMap((row) => {
      const stockActual = this.toNumeric(row.stock_actual);
      const stockMinimo = this.toNumeric(row.stock_min_bodega);
      if (stockMinimo <= 0 || stockActual > stockMinimo) return [];

      const producto = productMap.get(row.producto_id);
      const bodega = warehouseMap.get(row.bodega_id);
      const productoLabel = this.buildProductoLabel(producto) ?? row.producto_id;
      const bodegaLabel = this.buildBodegaLabel(bodega) ?? row.bodega_id;
      const isCritical = stockActual <= 0;

      return [
        {
          equipo_id: null,
          tipo_alerta: isCritical ? 'SIN_STOCK' : 'STOCK_BAJO_BODEGA',
          categoria: 'INVENTARIO' as AlertCategory,
          nivel: isCritical ? 'CRITICAL' : 'WARNING',
          origen: 'INVENTARIO' as AlertOrigin,
          referencia_tipo: 'STOCK_BODEGA',
          referencia: `STOCK_BODEGA:${row.id}`,
          detalle: `${productoLabel} · ${bodegaLabel} · stock ${stockActual.toFixed(
            2,
          )} / mínimo ${stockMinimo.toFixed(2)}`,
          payload_json: {
            stock_id: row.id,
            producto_id: row.producto_id,
            producto_codigo: producto?.codigo ?? null,
            producto_nombre: producto?.nombre ?? null,
            producto_label: productoLabel,
            bodega_id: row.bodega_id,
            bodega_codigo: bodega?.codigo ?? null,
            bodega_nombre: bodega?.nombre ?? null,
            bodega_label: bodegaLabel,
            stock_actual: stockActual,
            stock_min_bodega: stockMinimo,
            stock_max_bodega: this.toNumeric(row.stock_max_bodega),
            costo_promedio_bodega: this.toNumeric(row.costo_promedio_bodega),
          },
        },
      ];
    });
  }

  private async buildAlertCandidates() {
    const [programaciones, reportesDiarios, lubricantes, combustibles, inventario] =
      await Promise.all([
        this.buildProgramacionAlertCandidates(),
        this.buildReporteDiarioAlertCandidates(),
        this.buildLubricanteAlertCandidates(),
        this.buildFuelAlertCandidates(),
        this.buildInventoryAlertCandidates(),
      ]);

    return [
      ...programaciones,
      ...reportesDiarios,
      ...lubricantes,
      ...combustibles,
      ...inventario,
    ];
  }

  private async syncAlertCandidates(candidates: AlertCandidate[]) {
    const managedOrigins: AlertOrigin[] = [
      'SYSTEM',
      'PROGRAMACION',
      'REPORTE_DIARIO',
      'ANALISIS_LUBRICANTE',
      'COMBUSTIBLE',
      'INVENTARIO',
    ];
    const activeRows = await this.alertaRepo.find({
      where: {
        is_deleted: false,
        estado: In(['ABIERTA', 'EN_PROCESO']),
        origen: In(managedOrigins),
      },
      order: { fecha_generada: 'DESC', id: 'DESC' },
    });

    const activeMap = new Map<string, AlertaMantenimientoEntity>();
    const duplicateRows: AlertaMantenimientoEntity[] = [];
    for (const row of activeRows) {
      const key = this.buildAlertIdentity(row);
      if (!activeMap.has(key)) {
        activeMap.set(key, row);
      } else {
        duplicateRows.push(row);
      }
    }

    const now = new Date();
    const seen = new Set<string>();
    let created = 0;
    let updated = 0;
    let resolved = 0;

    for (const candidate of candidates) {
      const key = this.buildAlertIdentity(candidate);
      seen.add(key);
      const existing = activeMap.get(key);
      if (existing) {
        const nextDetalle = String(candidate.detalle || '').trim() || null;
        const preservedWorkOrders = this.extractAlertWorkOrderSnapshots(existing);
        const nextPayload = {
          ...(candidate.payload_json ?? {}),
          ...(preservedWorkOrders.length
            ? { work_orders: preservedWorkOrders }
            : {}),
        };
        const changed =
          existing.categoria !== candidate.categoria ||
          existing.nivel !== candidate.nivel ||
          existing.origen !== candidate.origen ||
          (existing.referencia_tipo ?? null) !==
            (candidate.referencia_tipo ?? null) ||
          (existing.referencia ?? null) !== (candidate.referencia ?? null) ||
          (existing.detalle ?? null) !== nextDetalle ||
          JSON.stringify(existing.payload_json ?? {}) !==
            JSON.stringify(nextPayload);

        existing.categoria = candidate.categoria;
        existing.nivel = candidate.nivel;
        existing.origen = candidate.origen;
        existing.referencia_tipo = candidate.referencia_tipo ?? null;
        existing.referencia = candidate.referencia ?? null;
        existing.detalle = nextDetalle;
        existing.payload_json = nextPayload;
        existing.ultima_evaluacion_at = now;
        existing.resolved_at = null;
        if (changed) {
          await this.alertaRepo.save(existing);
          updated += 1;
        }
        continue;
      }

      await this.alertaRepo.save(
        this.alertaRepo.create({
          equipo_id: candidate.equipo_id ?? null,
          tipo_alerta: candidate.tipo_alerta,
          categoria: candidate.categoria,
          nivel: candidate.nivel,
          origen: candidate.origen,
          referencia_tipo: candidate.referencia_tipo ?? null,
          referencia: candidate.referencia ?? null,
          detalle: candidate.detalle,
          payload_json: candidate.payload_json ?? {},
          fecha_generada: now,
          ultima_evaluacion_at: now,
          resolved_at: null,
          estado: 'ABIERTA',
        }),
      );
      created += 1;
    }

    const staleRows = [...activeMap.entries()]
      .filter(([key]) => !seen.has(key))
      .map(([, row]) => row)
      .concat(duplicateRows)
      .filter((row) => !this.hasLinkedWorkOrders(row));

    for (const row of staleRows) {
      row.estado = 'RESUELTA';
      row.resolved_at = now;
      row.ultima_evaluacion_at = now;
      await this.alertaRepo.save(row);
      resolved += 1;
    }

    return {
      total: candidates.length,
      created,
      updated,
      resolved,
      open: candidates.length,
    };
  }

  private async enrichAlertRows(rows: AlertaMantenimientoEntity[]) {
    const equipoIds = [...new Set(rows.map((row) => row.equipo_id).filter(Boolean))] as string[];
    const workOrderIds = [
      ...new Set(
        rows.flatMap((row) =>
          this.extractAlertWorkOrderSnapshots(row).map((item) => item.id),
        ),
      ),
    ] as string[];

    const [equipos, workOrders] = await Promise.all([
      equipoIds.length
        ? this.equipoRepo.find({ where: { id: In(equipoIds), is_deleted: false } })
        : Promise.resolve([] as EquipoEntity[]),
      workOrderIds.length
        ? this.woRepo.find({ where: { id: In(workOrderIds), is_deleted: false } })
        : Promise.resolve([] as WorkOrderEntity[]),
    ]);

    const equipoMap = new Map(equipos.map((row) => [row.id, row]));
    const workOrderMap = new Map(workOrders.map((row) => [row.id, row]));

    return [...rows]
      .map((row) => {
        const payload = (row.payload_json ?? {}) as Record<string, unknown>;
        const equipo = row.equipo_id ? equipoMap.get(row.equipo_id) : null;
        const linkedWorkOrders = this.extractAlertWorkOrderSnapshots(row)
          .map((snapshot) => {
            const persisted = workOrderMap.get(snapshot.id);
            if (!persisted) {
              return {
                ...snapshot,
                status_workflow: snapshot.status_workflow
                  ? this.normalizeWorkflowStatus(snapshot.status_workflow)
                  : null,
                label:
                  [snapshot.code, snapshot.title].filter(Boolean).join(' · ') ||
                  snapshot.id,
              };
            }

            const nextSnapshot = this.buildAlertWorkOrderSnapshot(persisted);
            return {
              ...nextSnapshot,
              label:
                `${persisted.code} · ${persisted.title}`.trim() || persisted.id,
            };
          })
          .filter((item) => item.id);
        const primaryWorkOrder =
          linkedWorkOrders[linkedWorkOrders.length - 1] ?? null;
        const workOrder = primaryWorkOrder;
        const equipoCodigoFallback =
          String(payload.equipo_codigo || payload.equipo_id || '').trim() || null;
        const equipoNombreFallback =
          String(payload.equipo_nombre || payload.equipo_codigo || '').trim() ||
          null;
        const equipoCodigo = equipo?.codigo ?? equipoCodigoFallback;
        const equipoNombre = equipo?.nombre ?? equipoNombreFallback;
        const equipoLabel = [equipoCodigo, equipoNombre]
          .filter(Boolean)
          .join(' - ');
        const workOrderLabel = workOrder
          ? `${workOrder.code} · ${workOrder.title}`
          : null;

        let referenciaResuelta =
          String(row.referencia || '').trim() || String(row.referencia_tipo || '').trim() || 'Sin referencia';
        if (row.referencia_tipo === 'PROGRAMACION') {
          referenciaResuelta =
            String(payload.procedimiento_nombre || payload.plan_nombre || payload.plan_codigo || referenciaResuelta);
        } else if (row.referencia_tipo === 'REPORTE_DIARIO') {
          referenciaResuelta =
            String(payload.reporte_codigo || payload.fecha_reporte || referenciaResuelta);
        } else if (row.referencia_tipo === 'ANALISIS_LUBRICANTE') {
          referenciaResuelta = String(payload.codigo || referenciaResuelta);
        } else if (row.referencia_tipo === 'COMBUSTIBLE') {
          referenciaResuelta = `Tanque ${String(payload.tanque || referenciaResuelta)}`;
        } else if (row.referencia_tipo === 'STOCK_BODEGA') {
          referenciaResuelta = [
            String(payload.producto_label || '').trim(),
            String(payload.bodega_label || '').trim(),
          ]
            .filter(Boolean)
            .join(' · ') || referenciaResuelta;
        }

        let title = `${row.tipo_alerta}${equipoLabel ? ` · ${equipoLabel}` : ''}`;
        let subtitle = String(row.detalle || '').trim();
        let accionSugerida = 'Revisar la condición y programar la acción correctiva.';

        if (row.origen === 'PROGRAMACION') {
          const planLabel =
            String(
              payload.procedimiento_nombre ||
                payload.plan_nombre ||
                payload.plan_codigo ||
                'Mantenimiento',
            ).trim();
          title = `${planLabel}${equipoLabel ? ` · ${equipoLabel}` : ''}`;
          accionSugerida =
            row.tipo_alerta === 'MANTENIMIENTO_VENCIDO'
              ? 'Generar o priorizar la OT del mantenimiento vencido.'
              : 'Preparar recursos y abrir la OT preventiva antes del vencimiento.';
        } else if (row.origen === 'REPORTE_DIARIO') {
          const mpg = String(payload.proximo_mpg || '').trim();
          title = `${mpg || 'Seguimiento MPG'}${
            equipoLabel ? ` · ${equipoLabel}` : ''
          }`;
          accionSugerida =
            row.tipo_alerta === 'REPORTE_DIARIO_VENCIDO'
              ? 'Validar el reporte diario y abrir la intervención correspondiente.'
              : 'Coordinar la atención antes de que el equipo quede vencido.';
        } else if (row.origen === 'ANALISIS_LUBRICANTE') {
          const compartimento = String(
            payload.compartimento_principal || 'Compartimento',
          ).trim();
          title = `Análisis de lubricante · ${compartimento}${
            equipoLabel ? ` · ${equipoLabel}` : ''
          }`;
          accionSugerida =
            'Revisar diagnóstico, tendencias y tomar muestra o intervención correctiva.';
        } else if (row.origen === 'COMBUSTIBLE') {
          title = `Combustible · ${String(payload.tanque || 'Tanque').trim()}`;
          accionSugerida =
            'Coordinar abastecimiento y confirmar que el tanque regrese sobre el mínimo.';
        } else if (row.origen === 'INVENTARIO') {
          title = [
            'Inventario',
            String(payload.producto_label || '').trim(),
            String(payload.bodega_label || '').trim(),
          ]
            .filter(Boolean)
            .join(' · ');
          accionSugerida =
            row.tipo_alerta === 'SIN_STOCK'
              ? 'Gestionar reposición inmediata o traslado entre bodegas.'
              : 'Revisar reabastecimiento antes de afectar mantenimiento u operación.';
        } else if (row.origen === 'BITACORA') {
          title = `Anomalía de datos${equipoLabel ? ` · ${equipoLabel}` : ''}`;
          accionSugerida =
            'Validar la bitácora y corregir la lectura antes de continuar.';
        }

        if (!subtitle) {
          subtitle = title;
        }
        if (workOrderLabel) {
          subtitle = `${subtitle} · OT ${workOrderLabel}`;
        }

        return {
          ...row,
          estado: this.normalizeAlertState(row.estado),
          nivel: this.normalizeAlertLevel(row.nivel),
          equipo_codigo: equipoCodigo,
          equipo_nombre: equipoNombre,
          equipo_label: equipoLabel || null,
          work_order_code: workOrder?.code ?? null,
          work_order_title: workOrderLabel,
          work_order_status: workOrder
            ? this.normalizeWorkflowStatus(workOrder.status_workflow)
            : null,
          work_order_count: linkedWorkOrders.length,
          work_orders: linkedWorkOrders,
          work_order_titles: linkedWorkOrders.map((item) => item.label),
          referencia_resuelta: referenciaResuelta,
          title,
          subtitle,
          accion_sugerida: accionSugerida,
        };
      })
      .sort((a, b) => {
        const stateDiff = this.alertStateRank(a.estado) - this.alertStateRank(b.estado);
        if (stateDiff !== 0) return stateDiff;
        const levelDiff = this.alertLevelRank(a.nivel) - this.alertLevelRank(b.nivel);
        if (levelDiff !== 0) return levelDiff;
        return (
          new Date(b.fecha_generada || 0).getTime() -
          new Date(a.fecha_generada || 0).getTime()
        );
      });
  }

  private async appendWorkOrderHistory(
    workOrderId: string,
    toStatus: string,
    note: string,
    options?: { fromStatus?: string | null; changedBy?: string | null },
  ) {
    return this.woHistoryRepo.save(
      this.woHistoryRepo.create({
        work_order_id: workOrderId,
        from_status: options?.fromStatus ?? null,
        to_status: toStatus,
        changed_by: options?.changedBy ?? null,
        note,
      }),
    );
  }

  private applyWorkflowDates(
    workOrder: WorkOrderEntity,
    previousStatus: string | null,
    nextStatus: string,
  ) {
    if (nextStatus === 'IN_PROGRESS' && !workOrder.started_at) {
      workOrder.started_at = new Date();
    }
    if (nextStatus === 'CLOSED' && !workOrder.closed_at) {
      workOrder.closed_at = new Date();
      if (!workOrder.started_at) workOrder.started_at = new Date();
    }
    if (previousStatus === 'CLOSED' && nextStatus !== 'CLOSED') {
      workOrder.closed_at = null;
    }
  }

  private addInterval(dateInput: string | Date, frequencyType: string, frequencyValue: number) {
    const date = new Date(dateInput);
    const type = String(frequencyType || 'DIAS').toUpperCase();
    const value = this.toNumeric(frequencyValue, 0);
    if (!value) return date;
    if (type === 'SEMANAS') {
      date.setDate(date.getDate() + value * 7);
      return date;
    }
    if (type === 'MESES') {
      date.setMonth(date.getMonth() + value);
      return date;
    }
    date.setDate(date.getDate() + value);
    return date;
  }

  private async recalculateProgramacionFields(
    programacion: ProgramacionPlanEntity,
    options?: { persist?: boolean },
  ) {
    const equipo = await this.findEquipoOrFail(programacion.equipo_id);
    const plan = await this.findOneOrFail(this.planRepo, {
      id: programacion.plan_id,
      is_deleted: false,
    });
    const procedimiento = await this.resolveProcedimientoFromPlan(plan);

    const freqType = String(plan.frecuencia_tipo || 'HORAS').toUpperCase();
    const freqValue = this.toNumeric(plan.frecuencia_valor, 0);
    const patch: Partial<ProgramacionPlanEntity> = {};

    if (freqType === 'HORAS') {
      const baseHours =
        programacion.ultima_ejecucion_horas != null
          ? this.toNumeric(programacion.ultima_ejecucion_horas)
          : this.toNumeric(equipo.horometro_actual);
      patch.proxima_horas = Number((baseHours + freqValue).toFixed(2));
      if (!programacion.ultima_ejecucion_horas && equipo.horometro_actual != null) {
        patch.ultima_ejecucion_horas = this.toNumeric(equipo.horometro_actual);
      }
    } else {
      const baseDate =
        programacion.ultima_ejecucion_fecha ||
        new Date().toISOString().slice(0, 10);
      patch.proxima_fecha = this
        .addInterval(baseDate, freqType, freqValue)
        .toISOString()
        .slice(0, 10);
      if (!programacion.ultima_ejecucion_fecha) {
        patch.ultima_ejecucion_fecha = baseDate;
      }
    }

    const currentHours = this.toNumeric(equipo.horometro_actual);
    const nextHours = patch.proxima_horas ?? programacion.proxima_horas ?? null;
    const nextDate = patch.proxima_fecha ?? programacion.proxima_fecha ?? null;
    const hoursRemaining = nextHours == null ? null : Number((this.toNumeric(nextHours) - currentHours).toFixed(2));
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const daysRemaining =
      nextDate == null
        ? null
        : Math.ceil((new Date(nextDate).getTime() - today.getTime()) / 86400000);
    const dueByHours = hoursRemaining != null && hoursRemaining <= 0;
    const dueByDate = daysRemaining != null && daysRemaining <= 0;
    const dueSoon =
      (hoursRemaining != null && hoursRemaining > 0 && hoursRemaining <= Math.max(1, freqValue * 0.1)) ||
      (daysRemaining != null && daysRemaining > 0 && daysRemaining <= 3);

    const enriched = {
      ...programacion,
      ...patch,
      equipo_nombre: equipo.nombre,
      equipo_codigo: equipo.codigo,
      plan_nombre: plan.nombre,
      plan_codigo: plan.codigo,
      procedimiento_id: procedimiento?.id ?? null,
      procedimiento_codigo: procedimiento?.codigo ?? null,
      procedimiento_nombre: procedimiento?.nombre ?? null,
      frecuencia_tipo: plan.frecuencia_tipo,
      frecuencia_valor: plan.frecuencia_valor,
      horometro_actual: currentHours,
      horas_restantes: hoursRemaining,
      dias_restantes: daysRemaining,
      estado_programacion: dueByHours || dueByDate ? 'VENCIDA' : dueSoon ? 'PROXIMA' : 'PROGRAMADA',
    };

    if (options?.persist !== false) {
      const changed = Object.entries(patch).some(([key, value]) => (programacion as any)[key] !== value);
      if (changed) {
        Object.assign(programacion, patch);
        await this.programacionRepo.save(programacion);
      }
    }

    return enriched;
  }

  private async enrichWorkOrder(workOrder: WorkOrderEntity) {
    const [equipo, plan] = await Promise.all([
      workOrder.equipment_id
        ? this.equipoRepo.findOne({ where: { id: workOrder.equipment_id, is_deleted: false } })
        : Promise.resolve(null),
      workOrder.plan_id
        ? this.planRepo.findOne({ where: { id: workOrder.plan_id, is_deleted: false } })
        : Promise.resolve(null),
    ]);
    const procedimientoIdFromPayload = String(
      (workOrder.valor_json as Record<string, unknown> | null | undefined)
        ?.procedimiento_id ?? '',
    ).trim();
    const procedimiento =
      (procedimientoIdFromPayload
        ? await this.procedimientoRepo.findOne({
            where: { id: procedimientoIdFromPayload, is_deleted: false },
          })
        : null) ?? (await this.resolveProcedimientoFromPlan(plan));

    return {
      ...workOrder,
      status_workflow: this.normalizeWorkflowStatus(workOrder.status_workflow),
      equipment_nombre: equipo?.nombre ?? null,
      equipment_codigo: equipo?.codigo ?? null,
      plan_nombre: plan?.nombre ?? null,
      plan_codigo: plan?.codigo ?? null,
      procedimiento_id: procedimiento?.id ?? null,
      procedimiento_codigo: procedimiento?.codigo ?? null,
      procedimiento_nombre: procedimiento?.nombre ?? null,
    };
  }

  async listEquipos(query: EquipoQueryDto) {
    const page = query.page ?? 1;
    const limit = Math.min(query.limit ?? 10, 100);
    const qb = this.equipoRepo
      .createQueryBuilder('e')
      .where('e.is_deleted = false');
    if (query.codigo)
      qb.andWhere('e.codigo ILIKE :codigo', { codigo: `%${query.codigo}%` });
    if (query.marca_id)
      qb.andWhere('e.marca_id = :marca_id', { marca_id: query.marca_id });
    if (query.location_id)
      qb.andWhere('e.location_id = :location_id', {
        location_id: query.location_id,
      });
    if (query.equipo_tipo_id)
      qb.andWhere('e.equipo_tipo_id = :equipo_tipo_id', {
        equipo_tipo_id: query.equipo_tipo_id,
      });
    if (query.estado_operativo)
      qb.andWhere('e.estado_operativo = :estado_operativo', {
        estado_operativo: query.estado_operativo,
      });
    if (query.criticidad)
      qb.andWhere('e.criticidad = :criticidad', {
        criticidad: query.criticidad,
      });
    const [data, total] = await qb
      .orderBy('e.created_at', 'DESC')
      .skip((page - 1) * limit)
      .take(limit)
      .getManyAndCount();
    return this.wrap(data, 'Equipos listados', { page, limit, total });
  }

  async getEquipo(id: string) {
    return this.wrap(await this.findEquipoOrFail(id), 'Equipo obtenido');
  }
  async createEquipo(dto: CreateEquipoDto) {
    return this.wrap(
      await this.equipoRepo.save(
        this.equipoRepo.create({
          ...dto,
          horometro_actual: dto.horometro_actual ?? 0,
        }),
      ),
      'Equipo creado',
    );
  }
  async updateEquipo(id: string, dto: UpdateEquipoDto) {
    const e = await this.findEquipoOrFail(id);
    Object.assign(e, dto);
    return this.wrap(await this.equipoRepo.save(e), 'Equipo actualizado');
  }
  async deleteEquipo(id: string) {
    const e = await this.findEquipoOrFail(id);
    e.is_deleted = true;
    e.deleted_at = new Date();
    await this.equipoRepo.save(e);
    return this.wrap(true, 'Equipo eliminado');
  }

  async listEquipoTipos(query: EquipoTipoQueryDto) {
    const page = query.page ?? 1;
    const limit = Math.min(query.limit ?? 10, 100);
    const qb = this.equipoTipoRepo
      .createQueryBuilder('t')
      .where('t.is_deleted = false');
    if (query.codigo)
      qb.andWhere('t.codigo ILIKE :codigo', { codigo: `%${query.codigo}%` });
    if (query.nombre)
      qb.andWhere('t.nombre ILIKE :nombre', { nombre: `%${query.nombre}%` });
    return this.wrap(
      await qb
        .skip((page - 1) * limit)
        .take(limit)
        .getMany(),
      'Tipos de equipo listados',
    );
  }
  async createEquipoTipo(dto: CreateEquipoTipoDto) {
    return this.wrap(
      await this.equipoTipoRepo.manager.save(
        EquipoTipoEntity,
        this.equipoTipoRepo.manager.create(EquipoTipoEntity, dto),
      ),
      'Tipo de equipo creado',
    );
  }
  async updateEquipoTipo(id: string, dto: UpdateEquipoTipoDto) {
    const t = await this.findOneOrFail(
      this.equipoTipoRepo.manager.getRepository(EquipoTipoEntity),
      {
        id,
        is_deleted: false,
      },
    );
    Object.assign(t, dto);
    return this.wrap(
      await this.equipoTipoRepo.manager.save(EquipoTipoEntity, t),
      'Tipo de equipo actualizado',
    );
  }
  async deleteEquipoTipo(id: string) {
    const t = await this.findOneOrFail(
      this.equipoTipoRepo.manager.getRepository(EquipoTipoEntity),
      {
        id,
        is_deleted: false,
      },
    );
    t.is_deleted = true;
    t.deleted_at = new Date();
    await this.equipoTipoRepo.manager.save(EquipoTipoEntity, t);
    return this.wrap(true, 'Tipo de equipo eliminado');
  }

  async listLocations(query: LocationQueryDto) {
    const page = query.page ?? 1;
    const limit = Math.min(query.limit ?? 10, 100);
    const qb = this.locationRepo
      .createQueryBuilder('l')
      .where('l.is_deleted = false');
    if (query.codigo)
      qb.andWhere('l.codigo ILIKE :codigo', { codigo: `%${query.codigo}%` });
    if (query.nombre)
      qb.andWhere('l.nombre ILIKE :nombre', { nombre: `%${query.nombre}%` });
    return this.wrap(
      await qb
        .skip((page - 1) * limit)
        .take(limit)
        .getMany(),
      'Locations listados',
    );
  }

  async getLocation(id: string) {
    return this.wrap(
      await this.findOneOrFail(
        this.locationRepo.manager.getRepository(LocationEntity),
        {
          id,
          is_deleted: false,
        },
      ),
      'Location obtenida',
    );
  }

  async createLocation(dto: CreateLocationDto) {
    return this.wrap(
      await this.locationRepo.manager.save(
        LocationEntity,
        this.locationRepo.manager.create(LocationEntity, dto),
      ),
      'Location creada',
    );
  }

  async updateLocation(id: string, dto: UpdateLocationDto) {
    const l = await this.findOneOrFail(
      this.locationRepo.manager.getRepository(LocationEntity),
      {
        id,
        is_deleted: false,
      },
    );
    Object.assign(l, dto);
    return this.wrap(
      await this.locationRepo.manager.save(LocationEntity, l),
      'Location actualizada',
    );
  }

  async deleteLocation(id: string) {
    const l = await this.findOneOrFail(
      this.locationRepo.manager.getRepository(LocationEntity),
      {
        id,
        is_deleted: false,
      },
    );
    l.is_deleted = true;
    l.deleted_at = new Date();
    await this.locationRepo.manager.save(LocationEntity, l);
    return this.wrap(true, 'Location eliminada');
  }

  async listBitacora(equipoId: string, range: DateRangeDto) {
    await this.findEquipoOrFail(equipoId);
    const qb = this.bitacoraRepo
      .createQueryBuilder('b')
      .where('b.equipo_id = :equipoId and b.is_deleted = false', { equipoId });
    if (range.from) qb.andWhere('b.fecha >= :from', { from: range.from });
    if (range.to) qb.andWhere('b.fecha <= :to', { to: range.to });
    return this.wrap(
      await qb.orderBy('b.fecha', 'DESC').getMany(),
      'Bitácora listada',
    );
  }

  async createBitacora(equipoId: string, dto: CreateBitacoraDto) {
    const equipo = await this.findEquipoOrFail(equipoId);
    const last = await this.bitacoraRepo.findOne({
      where: { equipo_id: equipoId, is_deleted: false },
      order: { fecha: 'DESC', created_at: 'DESC' },
    });
    if (last && Number(dto.horometro) < Number(last.horometro)) {
      await this.alertaRepo.save(
        this.alertaRepo.create({
          equipo_id: equipoId,
          tipo_alerta: 'ANOMALIA_HOROMETRO',
          categoria: 'DATOS',
          nivel: 'CRITICAL',
          origen: 'BITACORA',
          referencia_tipo: 'BITACORA',
          referencia: `BITACORA:${equipoId}:${dto.fecha}`,
          payload_json: {
            equipo_id: equipoId,
            fecha: dto.fecha,
            nuevo_horometro: this.toNumeric(dto.horometro),
            ultimo_horometro: this.toNumeric(last.horometro),
          },
          detalle: `Horómetro ${dto.horometro} menor al último ${last.horometro}`,
        }),
      );
      throw new ConflictException('El horómetro no puede retroceder');
    }
    const saved = await this.bitacoraRepo.save(
      this.bitacoraRepo.create({ ...dto, equipo_id: equipoId }),
    );
    equipo.horometro_actual = dto.horometro;
    equipo.fecha_ultima_lectura = new Date(dto.fecha);
    await this.equipoRepo.save(equipo);
    await this.triggerAlertRecalculation('bitacora');
    return this.wrap(saved, 'Bitácora creada');
  }

  async updateBitacora(id: string, dto: UpdateBitacoraDto) {
    const b = await this.findOneOrFail(this.bitacoraRepo, {
      id,
      is_deleted: false,
    });
    Object.assign(b, dto);
    const saved = await this.bitacoraRepo.save(b);
    const equipo = await this.findEquipoOrFail(saved.equipo_id);
    if (dto.horometro != null) equipo.horometro_actual = dto.horometro;
    if (dto.fecha) equipo.fecha_ultima_lectura = new Date(dto.fecha);
    await this.equipoRepo.save(equipo);
    await this.triggerAlertRecalculation('bitacora-update');
    return this.wrap(saved, 'Bitácora actualizada');
  }
  async deleteBitacora(id: string) {
    const b = await this.findOneOrFail(this.bitacoraRepo, {
      id,
      is_deleted: false,
    });
    b.is_deleted = true;
    await this.bitacoraRepo.save(b);
    return this.wrap(true, 'Bitácora eliminada');
  }

  async changeEstado(equipoId: string, dto: ChangeEstadoDto) {
    const equipo = await this.findEquipoOrFail(equipoId);
    const estadoCatalogo = await this.findOneOrFail(this.estadoCatalogoRepo, {
      id: dto.estado_id,
      is_deleted: false,
    });
    const current = await this.estadoRepo.findOne({
      where: { equipo_id: equipoId, fecha_fin: IsNull(), is_deleted: false },
      order: { fecha_inicio: 'DESC' },
    });
    if (current) {
      current.fecha_fin = new Date(dto.fecha_inicio);
      await this.estadoRepo.save(current);
    }
    const nuevo = await this.estadoRepo.save(
      this.estadoRepo.create({
        equipo_id: equipoId,
        estado_id: dto.estado_id,
        fecha_inicio: new Date(dto.fecha_inicio),
        motivo: dto.motivo,
      }),
    );
    equipo.estado_operativo = estadoCatalogo.codigo;
    await this.equipoRepo.save(equipo);
    return this.wrap(nuevo, 'Estado actualizado');
  }

  async listEstados(equipoId: string, range: DateRangeDto) {
    const qb = this.estadoRepo
      .createQueryBuilder('e')
      .where('e.equipo_id = :equipoId and e.is_deleted = false', { equipoId });
    if (range.from)
      qb.andWhere('e.fecha_inicio >= :from', { from: range.from });
    if (range.to) qb.andWhere('e.fecha_inicio <= :to', { to: range.to });
    return this.wrap(
      await qb.orderBy('e.fecha_inicio', 'DESC').getMany(),
      'Estados listados',
    );
  }

  createEvento(equipoId: string, dto: CreateEventoDto) {
    return this.wrap(
      this.eventoRepo.save(
        this.eventoRepo.create({ ...dto, equipo_id: equipoId }),
      ),
      'Evento creado',
    );
  }
  async listEventos(
    equipoId: string,
    range: DateRangeDto & { tipo_evento?: string },
  ) {
    const qb = this.eventoRepo
      .createQueryBuilder('e')
      .where('e.equipo_id = :equipoId and e.is_deleted = false', { equipoId });
    if (range.from)
      qb.andWhere('e.fecha_inicio >= :from', { from: range.from });
    if (range.to) qb.andWhere('e.fecha_inicio <= :to', { to: range.to });
    if (range.tipo_evento)
      qb.andWhere('e.tipo_evento = :tipo', { tipo: range.tipo_evento });
    return this.wrap(await qb.getMany(), 'Eventos listados');
  }

  async createPlan(dto: CreatePlanDto) {
    return this.wrap(
      await this.planRepo.save(this.planRepo.create(dto)),
      'Plan creado',
    );
  }
  async listPlanes() {
    return this.wrap(
      await this.planRepo.find({ where: { is_deleted: false } }),
      'Planes listados',
    );
  }
  async getPlan(id: string) {
    return this.wrap(
      await this.findOneOrFail(this.planRepo, { id, is_deleted: false }),
      'Plan obtenido',
    );
  }
  async updatePlan(id: string, dto: UpdatePlanDto) {
    const p = await this.findOneOrFail(this.planRepo, {
      id,
      is_deleted: false,
    });
    Object.assign(p, dto);
    return this.wrap(await this.planRepo.save(p), 'Plan actualizado');
  }
  async deletePlan(id: string) {
    const p = await this.findOneOrFail(this.planRepo, {
      id,
      is_deleted: false,
    });
    p.is_deleted = true;
    await this.planRepo.save(p);
    return this.wrap(true, 'Plan eliminado');
  }
  async createPlanTarea(planId: string, dto: CreatePlanTareaDto) {
    return this.wrap(
      await this.planTareaRepo.save(
        this.planTareaRepo.create({ ...dto, plan_id: planId }),
      ),
      'Tarea creada',
    );
  }
  async listPlanTareas(planId: string) {
    return this.wrap(
      await this.planTareaRepo.find({
        where: { plan_id: planId, is_deleted: false },
        order: { orden: 'ASC' },
      }),
      'Tareas listadas',
    );
  }
  async updatePlanTarea(id: string, dto: UpdatePlanTareaDto) {
    const t = await this.findOneOrFail(this.planTareaRepo, {
      id,
      is_deleted: false,
    });
    Object.assign(t, dto);
    return this.wrap(await this.planTareaRepo.save(t), 'Tarea actualizada');
  }
  async deletePlanTarea(id: string) {
    const t = await this.findOneOrFail(this.planTareaRepo, {
      id,
      is_deleted: false,
    });
    t.is_deleted = true;
    await this.planTareaRepo.save(t);
    return this.wrap(true, 'Tarea eliminada');
  }

  async createProgramacion(dto: CreateProgramacionDto) {
    await this.findEquipoOrFail(dto.equipo_id);
    let resolvedPlanId = dto.plan_id ?? null;
    if (dto.procedimiento_id) {
      const synced = await this.syncPlanFromProcedimiento(dto.procedimiento_id);
      resolvedPlanId = synced.plan.id;
    }
    if (!resolvedPlanId) {
      throw new BadRequestException(
        'Debes seleccionar una plantilla MPG o un plan operativo.',
      );
    }
    await this.findOneOrFail(this.planRepo, { id: resolvedPlanId, is_deleted: false });
    const entity = this.programacionRepo.create({
      equipo_id: dto.equipo_id,
      plan_id: resolvedPlanId,
      ultima_ejecucion_fecha: dto.ultima_ejecucion_fecha ?? null,
      ultima_ejecucion_horas: dto.ultima_ejecucion_horas ?? null,
      proxima_fecha: dto.proxima_fecha ?? null,
      proxima_horas: dto.proxima_horas ?? null,
      activo: dto.activo ?? true,
    });
    const saved = await this.programacionRepo.save(entity);
    const enriched = await this.recalculateProgramacionFields(saved);
    await this.publishInAppNotification({
      title: 'Nueva programación de mantenimiento',
      body: `${enriched.plan_nombre} programado para ${enriched.equipo_nombre}`,
      module: 'maintenance',
      entityType: 'programacion',
      entityId: saved.id,
      level: 'info',
    });
    await this.writeSecurityLog({
      description: `[PROGRAMACION:${saved.id}] Programación creada para equipo ${saved.equipo_id} y plan ${saved.plan_id}` ,
      typeLog: 'PROGRAMACION',
    });
    await this.registerProcessEvent({
      tipo_proceso: 'PROGRAMACION',
      accion: 'CREATED',
      referencia_tabla: 'tb_programacion_plan',
      referencia_id: saved.id,
      referencia_codigo: enriched.plan_codigo ?? saved.id,
      equipo_id: saved.equipo_id,
      title: 'Programación registrada',
      body: `${enriched.plan_nombre} para ${enriched.equipo_nombre}`,
      payload_kpi: {
        estado_programacion: enriched.estado_programacion,
      },
    });
    return this.wrap(enriched, 'Programación creada');
  }
  async listProgramaciones() {
    const rows = await this.programacionRepo.find({ where: { is_deleted: false, activo: true } });
    const data = await Promise.all(rows.map((row) => this.recalculateProgramacionFields(row, { persist: false })));
    data.sort((a: any, b: any) => {
      const left = a.proxima_fecha || '';
      const right = b.proxima_fecha || '';
      if (left && right) return String(left).localeCompare(String(right));
      return this.toNumeric(a.proxima_horas, 99999999) - this.toNumeric(b.proxima_horas, 99999999);
    });
    return this.wrap(data, 'Programaciones listadas');
  }
  async getProgramacion(id: string) {
    const row = await this.findOneOrFail(this.programacionRepo, {
      id,
      is_deleted: false,
    });
    return this.wrap(
      await this.recalculateProgramacionFields(row, { persist: false }),
      'Programación obtenida',
    );
  }
  async updateProgramacion(id: string, dto: UpdateProgramacionDto) {
    const p = await this.findOneOrFail(this.programacionRepo, {
      id,
      is_deleted: false,
    });
    let resolvedPlanId = dto.plan_id ?? p.plan_id;
    if (dto.procedimiento_id) {
      const synced = await this.syncPlanFromProcedimiento(dto.procedimiento_id);
      resolvedPlanId = synced.plan.id;
    }
    if (!resolvedPlanId) {
      throw new BadRequestException(
        'Debes seleccionar una plantilla MPG o un plan operativo.',
      );
    }
    Object.assign(p, {
      equipo_id: dto.equipo_id ?? p.equipo_id,
      plan_id: resolvedPlanId,
      ultima_ejecucion_fecha: dto.ultima_ejecucion_fecha ?? p.ultima_ejecucion_fecha ?? null,
      ultima_ejecucion_horas: dto.ultima_ejecucion_horas ?? p.ultima_ejecucion_horas ?? null,
      proxima_fecha: dto.proxima_fecha ?? p.proxima_fecha ?? null,
      proxima_horas: dto.proxima_horas ?? p.proxima_horas ?? null,
      activo: dto.activo ?? p.activo,
    });
    const saved = await this.programacionRepo.save(p);
    const enriched = await this.recalculateProgramacionFields(saved);
    await this.publishInAppNotification({
      title: 'Programación actualizada',
      body: `${enriched.plan_nombre} actualizado para ${enriched.equipo_nombre}`,
      module: 'maintenance',
      entityType: 'programacion',
      entityId: saved.id,
      level: 'info',
    });
    await this.writeSecurityLog({
      description: `[PROGRAMACION:${saved.id}] Programación actualizada`,
      typeLog: 'PROGRAMACION',
    });
    await this.registerProcessEvent({
      tipo_proceso: 'PROGRAMACION',
      accion: 'UPDATED',
      referencia_tabla: 'tb_programacion_plan',
      referencia_id: saved.id,
      referencia_codigo: enriched.plan_codigo ?? saved.id,
      equipo_id: saved.equipo_id,
      title: 'Programación actualizada',
      body: `${enriched.plan_nombre} para ${enriched.equipo_nombre}`,
      payload_kpi: {
        estado_programacion: enriched.estado_programacion,
      },
    });
    return this.wrap(enriched, 'Programación actualizada');
  }
  async deleteProgramacion(id: string) {
    const p = await this.findOneOrFail(this.programacionRepo, {
      id,
      is_deleted: false,
    });
    p.is_deleted = true;
    await this.programacionRepo.save(p);
    return this.wrap(true, 'Programación eliminada');
  }

  async listComponentes(query: ComponenteQueryDto) {
    const where: FindOptionsWhere<EquipoComponenteEntity> = {
      is_deleted: false,
    };
    if (query.equipo_id) where.equipo_id = query.equipo_id;
    return this.wrap(
      await this.equipoComponenteRepo.find({ where }),
      'Componentes listados',
    );
  }

  async getComponente(id: string) {
    return this.wrap(
      await this.findOneOrFail(this.equipoComponenteRepo, {
        id,
        is_deleted: false,
      }),
      'Componente obtenido',
    );
  }
  async createComponente(dto: CreateComponenteDto) {
    await this.findEquipoOrFail(dto.equipo_id);
    if (dto.parent_id)
      await this.findOneOrFail(this.equipoComponenteRepo, {
        id: dto.parent_id,
        is_deleted: false,
      });
    return this.wrap(
      await this.equipoComponenteRepo.save(
        this.equipoComponenteRepo.create(dto),
      ),
      'Componente creado',
    );
  }
  async updateComponente(id: string, dto: UpdateComponenteDto) {
    const item = await this.findOneOrFail(this.equipoComponenteRepo, {
      id,
      is_deleted: false,
    });
    Object.assign(item, dto);
    return this.wrap(
      await this.equipoComponenteRepo.save(item),
      'Componente actualizado',
    );
  }
  async deleteComponente(id: string) {
    const item = await this.findOneOrFail(this.equipoComponenteRepo, {
      id,
      is_deleted: false,
    });
    item.is_deleted = true;
    await this.equipoComponenteRepo.save(item);
    return this.wrap(true, 'Componente eliminado');
  }

  async listFallasCatalogo() {
    return this.wrap(
      await this.fallaRepo.find({ where: { is_deleted: false } }),
      'Fallas listadas',
    );
  }
  async getFallaCatalogo(id: string) {
    return this.wrap(
      await this.findOneOrFail(this.fallaRepo, { id, is_deleted: false }),
      'Falla obtenida',
    );
  }
  async createFallaCatalogo(dto: CreateFallaCatalogoDto) {
    return this.wrap(
      await this.fallaRepo.save(this.fallaRepo.create(dto)),
      'Falla creada',
    );
  }
  async updateFallaCatalogo(id: string, dto: UpdateFallaCatalogoDto) {
    const item = await this.findOneOrFail(this.fallaRepo, {
      id,
      is_deleted: false,
    });
    Object.assign(item, dto);
    return this.wrap(await this.fallaRepo.save(item), 'Falla actualizada');
  }
  async deleteFallaCatalogo(id: string) {
    const item = await this.findOneOrFail(this.fallaRepo, {
      id,
      is_deleted: false,
    });
    item.is_deleted = true;
    await this.fallaRepo.save(item);
    return this.wrap(true, 'Falla eliminada');
  }

  async listLecturas(equipoId?: string) {
    const where: FindOptionsWhere<LecturaEquipoEntity> = { is_deleted: false };
    if (equipoId) where.equipo_id = equipoId;
    return this.wrap(
      await this.lecturaRepo.find({ where }),
      'Lecturas listadas',
    );
  }
  async getLectura(id: string) {
    return this.wrap(
      await this.findOneOrFail(this.lecturaRepo, { id, is_deleted: false }),
      'Lectura obtenida',
    );
  }
  async createLectura(dto: CreateLecturaEquipoDto) {
    const equipo = await this.findEquipoOrFail(dto.equipo_id);
    const created = await this.lecturaRepo.save(
      this.lecturaRepo.create({
        ...dto,
        fecha: dto.fecha ? new Date(dto.fecha) : new Date(),
      }),
    );
    const tipo = String(dto.tipo || '').toUpperCase();
    if ((tipo.includes('HORO') || tipo.includes('HORA')) && dto.valor != null) {
      equipo.horometro_actual = dto.valor;
      equipo.fecha_ultima_lectura = created.fecha;
      await this.equipoRepo.save(equipo);
      await this.triggerAlertRecalculation('lectura');
    }
    return this.wrap(created, 'Lectura creada');
  }
  async updateLectura(id: string, dto: UpdateLecturaEquipoDto) {
    const item = await this.findOneOrFail(this.lecturaRepo, {
      id,
      is_deleted: false,
    });
    Object.assign(item, {
      ...dto,
      fecha: dto.fecha ? new Date(dto.fecha) : item.fecha,
    });
    const saved = await this.lecturaRepo.save(item);
    const tipo = String(saved.tipo || '').toUpperCase();
    if ((tipo.includes('HORO') || tipo.includes('HORA')) && saved.valor != null) {
      const equipo = await this.findEquipoOrFail(saved.equipo_id);
      equipo.horometro_actual = this.toNumeric(saved.valor);
      equipo.fecha_ultima_lectura = saved.fecha;
      await this.equipoRepo.save(equipo);
      await this.triggerAlertRecalculation('lectura-update');
    }
    return this.wrap(saved, 'Lectura actualizada');
  }
  async deleteLectura(id: string) {
    const item = await this.findOneOrFail(this.lecturaRepo, {
      id,
      is_deleted: false,
    });
    item.is_deleted = true;
    await this.lecturaRepo.save(item);
    return this.wrap(true, 'Lectura eliminada');
  }

  async listLubricaciones(equipoId?: string) {
    const where: FindOptionsWhere<LubricacionPuntoEntity> = {
      is_deleted: false,
    };
    if (equipoId) where.equipo_id = equipoId;
    return this.wrap(
      await this.lubricacionRepo.find({ where }),
      'Puntos de lubricación listados',
    );
  }
  async getLubricacion(id: string) {
    return this.wrap(
      await this.findOneOrFail(this.lubricacionRepo, { id, is_deleted: false }),
      'Punto de lubricación obtenido',
    );
  }
  async createLubricacion(dto: CreateLubricacionPuntoDto) {
    await this.findEquipoOrFail(dto.equipo_id);
    if (dto.componente_id)
      await this.findOneOrFail(this.equipoComponenteRepo, {
        id: dto.componente_id,
        is_deleted: false,
      });
    return this.wrap(
      await this.lubricacionRepo.save(this.lubricacionRepo.create(dto)),
      'Punto de lubricación creado',
    );
  }
  async updateLubricacion(id: string, dto: UpdateLubricacionPuntoDto) {
    const item = await this.findOneOrFail(this.lubricacionRepo, {
      id,
      is_deleted: false,
    });
    Object.assign(item, dto);
    return this.wrap(
      await this.lubricacionRepo.save(item),
      'Punto de lubricación actualizado',
    );
  }
  async deleteLubricacion(id: string) {
    const item = await this.findOneOrFail(this.lubricacionRepo, {
      id,
      is_deleted: false,
    });
    item.is_deleted = true;
    await this.lubricacionRepo.save(item);
    return this.wrap(true, 'Punto de lubricación eliminado');
  }

  async listProcedimientosPlantilla() {
    const rows = await this.procedimientoRepo.find({
      where: { is_deleted: false },
      order: { updated_at: 'DESC', created_at: 'DESC' },
    });
    return this.wrap(
      await Promise.all(rows.map((row) => this.buildProcedimientoPayload(row))),
      'Procedimientos plantilla listados',
    );
  }

  async getProcedimientoPlantilla(id: string) {
    const row = await this.findOneOrFail(this.procedimientoRepo, {
      id,
      is_deleted: false,
    });
    return this.wrap(
      await this.buildProcedimientoPayload(row),
      'Procedimiento plantilla obtenido',
    );
  }

  async createProcedimientoPlantilla(dto: CreateProcedimientoPlantillaDto) {
    let resolution = await this.resolveRequestedProcedimientoPlantillaCode(
      dto.codigo,
    );
    let savedId: string | null = null;

    for (let attempt = 0; attempt < 3; attempt += 1) {
      try {
        savedId = await this.dataSource.transaction(async (manager) => {
          const procedimientoRepo = manager.getRepository(
            ProcedimientoPlantillaEntity,
          );
          const actividadRepo = manager.getRepository(
            ProcedimientoActividadEntity,
          );

          const row = await procedimientoRepo.save(
            procedimientoRepo.create({
              codigo: resolution.resolvedCode,
              nombre: dto.nombre,
              tipo_proceso: dto.tipo_proceso,
              documento_referencia: dto.documento_referencia ?? null,
              version: dto.version ?? null,
              clase_mantenimiento: dto.clase_mantenimiento ?? null,
              frecuencia_horas: dto.frecuencia_horas ?? null,
              objetivo: dto.objetivo ?? null,
              precauciones: this.normalizeStringArray(dto.precauciones),
              herramientas: this.normalizeStringArray(dto.herramientas),
              materiales: this.normalizeMaterialIdArray(dto.materiales),
              responsabilidades: this.normalizeStringArray(dto.responsabilidades),
            }),
          );

          if (dto.actividades?.length) {
            await actividadRepo.save(
              dto.actividades.map((actividad, index) =>
                actividadRepo.create({
                  procedimiento_id: row.id,
                  orden: actividad.orden ?? index + 1,
                  fase: actividad.fase ?? null,
                  actividad: actividad.actividad,
                  detalle: actividad.detalle ?? null,
                  requiere_permiso: actividad.requiere_permiso ?? false,
                  requiere_epp: actividad.requiere_epp ?? false,
                  requiere_bloqueo: actividad.requiere_bloqueo ?? false,
                  requiere_evidencia: actividad.requiere_evidencia ?? false,
                  meta: actividad.meta ?? {},
                }),
              ),
            );
          }

          return row.id;
        });
        break;
      } catch (error: any) {
        if (!this.isDuplicateCodigoError(error) || attempt >= 2) {
          throw error;
        }
        resolution = {
          requestedCode: resolution.requestedCode ?? dto.codigo ?? null,
          resolvedCode: await this.generateNextProcedimientoPlantillaCode(),
          codeWasReassigned: true,
          reassignmentReason:
            resolution.reassignmentReason ||
            'El código solicitado ya no estaba disponible al momento de guardar.',
        };
      }
    }

    if (!savedId) {
      throw new ConflictException(
        'No se pudo generar un código único para la plantilla MPG.',
      );
    }

    const saved = await this.findOneOrFail(this.procedimientoRepo, {
      id: savedId,
      is_deleted: false,
    });
    await this.syncPlanFromProcedimiento(saved.id);
    const payload = await this.buildProcedimientoPayload(saved);
    await this.registerProcessEvent({
      tipo_proceso: 'PROCEDIMIENTO_PLANTILLA',
      accion: 'CREATED',
      referencia_tabla: 'tb_procedimiento_plantilla',
      referencia_id: saved.id,
      referencia_codigo: saved.codigo,
      title: 'Nueva plantilla de procedimiento',
      body: `${saved.codigo} - ${saved.nombre}`,
      payload_kpi: {
        actividades: payload.actividades.length,
        tipo_proceso: saved.tipo_proceso,
      },
    });
    return this.wrap(
      {
        ...payload,
        requested_code: resolution.requestedCode,
        code_was_reassigned: resolution.codeWasReassigned,
        code_reassignment_reason: resolution.reassignmentReason,
      },
      'Procedimiento plantilla creado',
    );
  }

  async updateProcedimientoPlantilla(id: string, dto: UpdateProcedimientoPlantillaDto) {
    await this.findOneOrFail(this.procedimientoRepo, { id, is_deleted: false });

    await this.dataSource.transaction(async (manager) => {
      const procedimientoRepo = manager.getRepository(ProcedimientoPlantillaEntity);
      const actividadRepo = manager.getRepository(ProcedimientoActividadEntity);
      const row = await procedimientoRepo.findOne({
        where: { id, is_deleted: false },
      });
      if (!row) throw new NotFoundException('Procedimiento plantilla no encontrado');

      Object.assign(row, {
        codigo: row.codigo,
        nombre: dto.nombre ?? row.nombre,
        tipo_proceso: dto.tipo_proceso ?? row.tipo_proceso,
        documento_referencia: dto.documento_referencia ?? row.documento_referencia ?? null,
        version: dto.version ?? row.version ?? null,
        clase_mantenimiento: dto.clase_mantenimiento ?? row.clase_mantenimiento ?? null,
        frecuencia_horas: dto.frecuencia_horas ?? row.frecuencia_horas ?? null,
        objetivo: dto.objetivo ?? row.objetivo ?? null,
        precauciones: dto.precauciones
          ? this.normalizeStringArray(dto.precauciones)
          : row.precauciones,
        herramientas: dto.herramientas
          ? this.normalizeStringArray(dto.herramientas)
          : row.herramientas,
        materiales: dto.materiales
          ? this.normalizeMaterialIdArray(dto.materiales)
          : row.materiales,
        responsabilidades: dto.responsabilidades
          ? this.normalizeStringArray(dto.responsabilidades)
          : row.responsabilidades,
      });
      await procedimientoRepo.save(row);

      if (dto.actividades) {
        const current = await actividadRepo.find({
          where: { procedimiento_id: row.id, is_deleted: false },
        });
        for (const item of current) item.is_deleted = true;
        if (current.length) await actividadRepo.save(current);

        if (dto.actividades.length) {
          await actividadRepo.save(
            dto.actividades.map((actividad, index) =>
              actividadRepo.create({
                procedimiento_id: row.id,
                orden: actividad.orden ?? index + 1,
                fase: actividad.fase ?? null,
                actividad: actividad.actividad,
                detalle: actividad.detalle ?? null,
                requiere_permiso: actividad.requiere_permiso ?? false,
                requiere_epp: actividad.requiere_epp ?? false,
                requiere_bloqueo: actividad.requiere_bloqueo ?? false,
                requiere_evidencia: actividad.requiere_evidencia ?? false,
                meta: actividad.meta ?? {},
              }),
            ),
          );
        }
      }
    });

    const saved = await this.findOneOrFail(this.procedimientoRepo, { id, is_deleted: false });
    await this.syncPlanFromProcedimiento(saved.id);
    const payload = await this.buildProcedimientoPayload(saved);
    await this.registerProcessEvent({
      tipo_proceso: 'PROCEDIMIENTO_PLANTILLA',
      accion: 'UPDATED',
      referencia_tabla: 'tb_procedimiento_plantilla',
      referencia_id: saved.id,
      referencia_codigo: saved.codigo,
      title: 'Plantilla de procedimiento actualizada',
      body: `${saved.codigo} - ${saved.nombre}`,
      payload_kpi: {
        actividades: payload.actividades.length,
        tipo_proceso: saved.tipo_proceso,
      },
    });
    return this.wrap(payload, 'Procedimiento plantilla actualizado');
  }

  async deleteProcedimientoPlantilla(id: string) {
    const row = await this.findOneOrFail(this.procedimientoRepo, { id, is_deleted: false });
    row.is_deleted = true;
    await this.procedimientoRepo.save(row);
    await this.softDeleteRows(this.procedimientoActividadRepo, {
      procedimiento_id: row.id,
      is_deleted: false,
    });
    return this.wrap(true, 'Procedimiento plantilla eliminado');
  }

  async listAnalisisLubricante() {
    const rows = await this.analisisLubricanteRepo.find({
      where: { is_deleted: false },
      order: { fecha_reporte: 'DESC', created_at: 'DESC' },
    });
    return this.wrap(
      await Promise.all(rows.map((row) => this.buildAnalisisLubricantePayload(row))),
      'Análisis de lubricante listados',
    );
  }

  async getAnalisisLubricante(id: string) {
    const row = await this.findOneOrFail(this.analisisLubricanteRepo, {
      id,
      is_deleted: false,
    });
    return this.wrap(
      await this.buildAnalisisLubricantePayload(row),
      'Análisis de lubricante obtenido',
    );
  }

  async listAnalisisLubricanteCatalog(
    query: AnalisisLubricanteCatalogQueryDto,
  ) {
    const rows = await this.analisisLubricanteRepo.find({
      where: { is_deleted: false },
      order: { fecha_reporte: 'DESC', fecha_muestra: 'DESC', created_at: 'DESC' },
      take: 500,
    });

    const catalog = new Map<
      string,
      {
        key: string;
        lubricante: string;
        marca_lubricante: string | null;
        lubricante_codigo: string | null;
        total_analisis: number;
        ultima_fecha_reporte: string | null;
        ultimo_codigo: string | null;
        codigos_analisis: string[];
        clientes: string[];
        compartimentos: string[];
      }
    >();

    for (const row of rows) {
      const identity = this.resolveLubricantIdentity(row);
      if (!identity.lubricante_lookup_key || !identity.lubricante) continue;
      const existing = catalog.get(identity.lubricante_lookup_key) ?? {
        key: identity.lubricante_lookup_key,
        lubricante: identity.lubricante,
        marca_lubricante: identity.marca_lubricante,
        lubricante_codigo: identity.lubricante_codigo,
        total_analisis: 0,
        ultima_fecha_reporte: null,
        ultimo_codigo: null,
        codigos_analisis: [],
        clientes: [],
        compartimentos: [],
      };

      existing.total_analisis += 1;
      const referenceDate = this.resolveAnalisisFechaReferencia(row);
      if (
        referenceDate &&
        (!existing.ultima_fecha_reporte ||
          new Date(referenceDate) > new Date(existing.ultima_fecha_reporte))
      ) {
        existing.ultima_fecha_reporte = referenceDate;
        existing.ultimo_codigo = row.codigo;
      }
      if (row.codigo && !existing.codigos_analisis.includes(row.codigo)) {
        existing.codigos_analisis.push(row.codigo);
      }
      if (row.cliente && !existing.clientes.includes(row.cliente)) {
        existing.clientes.push(row.cliente);
      }
      if (
        row.compartimento_principal &&
        !existing.compartimentos.includes(row.compartimento_principal)
      ) {
        existing.compartimentos.push(row.compartimento_principal);
      }
      catalog.set(identity.lubricante_lookup_key, existing);
    }

    const search = this.normalizeSearchToken(query.search);
    const items = [...catalog.values()]
      .filter((item) => {
        if (!search) return true;
        const haystack = this.normalizeSearchToken(
          [
            item.lubricante_codigo,
            item.lubricante,
            item.marca_lubricante,
            ...item.codigos_analisis,
          ]
            .filter(Boolean)
            .join(' '),
        );
        return haystack.includes(search);
      })
      .sort((a, b) => {
        const dateA = a.ultima_fecha_reporte
          ? new Date(a.ultima_fecha_reporte).getTime()
          : 0;
        const dateB = b.ultima_fecha_reporte
          ? new Date(b.ultima_fecha_reporte).getTime()
          : 0;
        if (dateB !== dateA) return dateB - dateA;
        return a.lubricante.localeCompare(b.lubricante);
      });

    return this.wrap(items, 'Catálogo de lubricantes generado');
  }

  async getAnalisisLubricanteDashboard(
    query: AnalisisLubricanteDashboardQueryDto,
  ) {
    const rows = await this.analisisLubricanteRepo.find({
      where: { is_deleted: false },
      order: { fecha_reporte: 'ASC', fecha_muestra: 'ASC', created_at: 'ASC' },
    });

    const referencedRow = query.codigo
      ? rows.find(
          (item) =>
            this.normalizeSearchToken(item.codigo) ===
            this.normalizeSearchToken(query.codigo),
        ) ?? null
      : null;
    const referenceIdentity = this.resolveLubricantIdentity(referencedRow);
    const targetLookup = this.normalizeSearchToken(
      referenceIdentity.lubricante ?? query.lubricante ?? query.codigo ?? '',
    );
    const targetBrand = this.normalizeSearchToken(query.marca_lubricante);
    const targetCompartimento = this.normalizeSearchToken(query.compartimento);
    const range = this.resolveDashboardDateRange(
      query.periodo,
      query.from,
      query.to,
    );

    const matchingRows = rows.filter((row) => {
      const identity = this.resolveLubricantIdentity(row);
      const dateValue = this.resolveAnalisisFechaReferencia(row);
      const rowCompartimento = this.normalizeSearchToken(
        row.compartimento_principal,
      );
      const rowSearch = this.normalizeSearchToken(
        [
          identity.lubricante_codigo,
          identity.lubricante,
          identity.marca_lubricante,
          row.codigo,
        ]
          .filter(Boolean)
          .join(' '),
      );

      if (targetLookup && !rowSearch.includes(targetLookup)) return false;
      if (
        targetBrand &&
        !this.normalizeSearchToken(identity.marca_lubricante).includes(
          targetBrand,
        )
      ) {
        return false;
      }
      if (targetCompartimento && rowCompartimento !== targetCompartimento) {
        return false;
      }
      if (
        range.from &&
        dateValue &&
        new Date(dateValue) < new Date(range.from)
      ) {
        return false;
      }
      if (
        range.to &&
        dateValue &&
        new Date(dateValue) > new Date(range.to)
      ) {
        return false;
      }
      return true;
    });

    const analyses = await Promise.all(
      matchingRows.map((row) => this.buildAnalisisLubricantePayload(row)),
    );
    const sortedAnalyses = [...analyses].sort((a, b) => {
      const refA = this.resolveAnalisisFechaReferencia(a);
      const refB = this.resolveAnalisisFechaReferencia(b);
      const dateA = refA ? new Date(refA).getTime() : 0;
      const dateB = refB ? new Date(refB).getTime() : 0;
      return dateA - dateB;
    });

    const latestAnalysis = sortedAnalyses[sortedAnalyses.length - 1] ?? null;
    const selectedIdentity = this.resolveLubricantIdentity(latestAnalysis);
    const sampleHistory = sortedAnalyses.map((item) => ({
      id: item.id,
      codigo: item.codigo,
      fecha_reporte: item.fecha_reporte ?? null,
      fecha_muestra: item.fecha_muestra ?? null,
      numero_muestra: item.sample_info?.numero_muestra ?? null,
      compartimento_principal: item.compartimento_principal ?? null,
      estado_diagnostico: item.estado_diagnostico ?? 'NORMAL',
      condicion:
        item.sample_info?.condicion ?? item.estado_diagnostico ?? 'NORMAL',
      horas_equipo: item.sample_info?.horas_equipo ?? null,
      horas_lubricante: item.sample_info?.horas_lubricante ?? null,
    }));

    const seriesMap = new Map<
      string,
      {
        key: string;
        label: string;
        group: string;
        group_label: string;
        chart_group: string;
        unit: string | null;
        points: Array<Record<string, unknown>>;
      }
    >();

    for (const analysis of sortedAnalyses) {
      const referenceDate = this.resolveAnalisisFechaReferencia(analysis);
      for (const detail of analysis.detalles ?? []) {
        const currentValue =
          detail.resultado_numerico == null
            ? null
            : Number(detail.resultado_numerico);
        if (currentValue == null || !Number.isFinite(currentValue)) continue;

        const key = String(detail.parametro_key || detail.parametro || '').trim();
        if (!key) continue;
        if (!seriesMap.has(key)) {
          seriesMap.set(key, {
            key,
            label: detail.parametro_label ?? detail.parametro ?? key,
            group: detail.grupo ?? 'OTROS_ELEMENTOS',
            group_label: detail.grupo_label ?? 'Otros elementos',
            chart_group: detail.chart_group ?? 'otros',
            unit: detail.unidad ?? null,
            points: [],
          });
        }
        seriesMap.get(key)?.points.push({
          analisis_id: analysis.id,
          codigo: analysis.codigo,
          fecha: referenceDate,
          numero_muestra: analysis.sample_info?.numero_muestra ?? null,
          compartimento: detail.compartimento ?? analysis.compartimento_principal,
          valor: Number(currentValue.toFixed(4)),
          linea_base: detail.linea_base_resuelta ?? detail.linea_base ?? null,
          tendencia: detail.delta_valor ?? detail.tendencia ?? null,
          nivel_alerta: detail.nivel_alerta ?? 'NORMAL',
        });
      }
    }

    const chartSections = [
      { key: 'estado', title: 'Estado del lubricante' },
      { key: 'desgaste', title: 'Desgaste del equipo' },
      { key: 'contaminacion', title: 'Contaminación del lubricante' },
      { key: 'otros', title: 'Otros indicadores' },
    ].map((section) => ({
      ...section,
      metrics: [...seriesMap.values()]
        .filter((item) => item.chart_group === section.key)
        .sort((a, b) => {
          const defA = this.getLubricantMetricDefinition(a.label);
          const defB = this.getLubricantMetricDefinition(b.label);
          return (defA?.order ?? 999) - (defB?.order ?? 999);
        }),
    }));

    const globalLubricants = new Set(
      rows
        .map((item) => this.resolveLubricantIdentity(item).lubricante_lookup_key)
        .filter(Boolean),
    ).size;

    return this.wrap(
      {
        filters: range,
        selected: latestAnalysis
          ? {
              lubricante: selectedIdentity.lubricante,
              marca_lubricante: selectedIdentity.marca_lubricante,
              lubricante_codigo: selectedIdentity.lubricante_codigo,
              total_analisis: sortedAnalyses.length,
              compartimentos: [
                ...new Set(
                  sortedAnalyses
                    .map((item) => item.compartimento_principal)
                    .filter(Boolean),
                ),
              ],
              clientes: [
                ...new Set(
                  sortedAnalyses.map((item) => item.cliente).filter(Boolean),
                ),
              ],
            }
          : null,
        metrics: {
          lubricantes_registrados: globalLubricants,
          analisis_filtrados: sortedAnalyses.length,
          compartimentos_monitoreados: new Set(
            sortedAnalyses
              .map((item) => item.compartimento_principal)
              .filter(Boolean),
          ).size,
          anormales: sortedAnalyses.filter(
            (item) =>
              this.normalizeLubricantCondition(item.estado_diagnostico) ===
              'ANORMAL',
          ).length,
          precauciones: sortedAnalyses.filter(
            (item) =>
              this.normalizeLubricantCondition(item.estado_diagnostico) ===
              'PRECAUCION',
          ).length,
          sin_dato: sortedAnalyses.filter(
            (item) =>
              this.normalizeLubricantCondition(item.estado_diagnostico) ===
              'N/D',
          ).length,
        },
        timeline: sampleHistory,
        latest_analysis: latestAnalysis,
        detail_groups: latestAnalysis?.detalle_grupos ?? [],
        chart_sections: chartSections,
        series: [...seriesMap.values()],
      },
      'Dashboard de análisis de lubricante generado',
    );
  }

  async createAnalisisLubricante(
    dto: CreateAnalisisLubricanteDto,
    options: AnalisisLubricanteSaveOptions = {},
  ) {
    let resolution = await this.resolveRequestedAnalisisLubricanteCode(
      dto.codigo,
    );
    let savedId: string | null = null;
    const equipmentContext = await this.resolveAnalisisEquipmentContext(
      dto.equipo_id ?? null,
    );
    const basePayload = { ...((dto.payload_json ?? {}) as Record<string, unknown>) };
    const baseSampleInfo = this.extractAnalysisSampleInfo(basePayload);
    const payloadJson = this.mergeLubricantSampleInfo(basePayload, {
      ...baseSampleInfo,
      condicion: baseSampleInfo.condicion,
      equipo_marca: equipmentContext.marcaNombre ?? baseSampleInfo.equipo_marca,
    });
    const lubricanteIdentity = this.resolveLubricantIdentity({
      lubricante: dto.lubricante ?? null,
      marca_lubricante: dto.marca_lubricante ?? null,
      equipo_codigo: equipmentContext.equipo?.codigo ?? dto.equipo_codigo ?? null,
      equipo_nombre: equipmentContext.equipo?.nombre ?? dto.equipo_nombre ?? null,
      payload_json: payloadJson,
    });
    const preparedDetalles = await this.prepareAnalisisDetallesForSave({
      lubricante: lubricanteIdentity.lubricante,
      compartimento: dto.compartimento_principal ?? null,
      detalles: dto.detalles,
    });
    const inferredState = this.inferAnalisisStateFromDetails(
      preparedDetalles,
      dto.estado_diagnostico ?? baseSampleInfo.condicion,
    );
    const finalPayloadJson = this.mergeLubricantSampleInfo(payloadJson, {
      ...baseSampleInfo,
      condicion: dto.estado_diagnostico
        ? this.normalizeLubricantCondition(dto.estado_diagnostico)
        : baseSampleInfo.condicion,
      equipo_marca: equipmentContext.marcaNombre ?? baseSampleInfo.equipo_marca,
    });
    Object.assign(finalPayloadJson, {
      lubricante: lubricanteIdentity.lubricante,
      marca_lubricante: lubricanteIdentity.marca_lubricante,
    });
    const resolvedDiagnostic =
      String(dto.diagnostico ?? '').trim() ||
      this.buildLubricantDiagnosticText(preparedDetalles, inferredState);

    for (let attempt = 0; attempt < 3; attempt += 1) {
      try {
        savedId = await this.dataSource.transaction(async (manager) => {
          const analisisRepo = manager.getRepository(AnalisisLubricanteEntity);
          const detalleRepo = manager.getRepository(
            AnalisisLubricanteDetalleEntity,
          );
          const row = await analisisRepo.save(
            analisisRepo.create({
              codigo: resolution.resolvedCode,
              cliente: dto.cliente ?? null,
              equipo_id: dto.equipo_id ?? null,
              lubricante: lubricanteIdentity.lubricante,
              marca_lubricante: lubricanteIdentity.marca_lubricante,
              equipo_codigo: equipmentContext.equipo?.codigo ?? dto.equipo_codigo ?? null,
              equipo_nombre: equipmentContext.equipo?.nombre ?? dto.equipo_nombre ?? null,
              compartimento_principal: dto.compartimento_principal ?? null,
              fecha_muestra: this.toDateOnlyString(dto.fecha_muestra),
              fecha_reporte: this.toDateOnlyString(dto.fecha_reporte),
              diagnostico: resolvedDiagnostic,
              estado_diagnostico: inferredState,
              documento_origen: dto.documento_origen ?? null,
              payload_json: finalPayloadJson,
            }),
          );

          if (preparedDetalles.length) {
            await detalleRepo.save(
              preparedDetalles.map((detalle, index) =>
                detalleRepo.create({
                  analisis_id: row.id,
                  compartimento: detalle.compartimento,
                  numero_muestra: detalle.numero_muestra ?? null,
                  parametro: detalle.parametro,
                  resultado_numerico: detalle.resultado_numerico ?? null,
                  resultado_texto: detalle.resultado_texto ?? null,
                  unidad: detalle.unidad ?? null,
                  linea_base: detalle.linea_base ?? null,
                  nivel_alerta: detalle.nivel_alerta ?? 'NORMAL',
                  tendencia: detalle.tendencia ?? null,
                  observacion: detalle.observacion ?? null,
                  orden: detalle.orden ?? index + 1,
                }),
              ),
            );
          }

          return row.id;
        });
        break;
      } catch (error: any) {
        if (!this.isDuplicateCodigoError(error) || attempt >= 2) {
          throw error;
        }
        resolution = {
          requestedCode: resolution.requestedCode ?? dto.codigo ?? null,
          resolvedCode: await this.generateNextAnalisisLubricanteCode(),
          codeWasReassigned: true,
          reassignmentReason:
            resolution.reassignmentReason ||
            'El código solicitado ya no estaba disponible al momento de guardar.',
        };
      }
    }

    if (!savedId) {
      throw new ConflictException(
        'No se pudo generar un código único para el análisis de lubricante.',
      );
    }

    const saved = await this.findOneOrFail(this.analisisLubricanteRepo, {
      id: savedId,
      is_deleted: false,
    });
    const payload = await this.buildAnalisisLubricantePayload(saved);
    if (!options.skipProcessEvent) {
      await this.registerProcessEvent({
      tipo_proceso: 'ANALISIS_LUBRICANTE',
      accion: 'CREATED',
      referencia_tabla: 'tb_analisis_lubricante',
      referencia_id: saved.id,
      referencia_codigo: saved.codigo,
      equipo_id: saved.equipo_id ?? null,
      title: 'Nuevo análisis de lubricante',
      body: `${saved.codigo}${
        lubricanteIdentity.lubricante ? ` · ${lubricanteIdentity.lubricante}` : ''
      }`,
      level:
        ['ANORMAL', 'PRECAUCION'].includes(
          this.normalizeLubricantCondition(saved.estado_diagnostico),
        )
          ? 'warning'
          : 'info',
      payload_kpi: {
        detalles: payload.detalles.length,
        estado: saved.estado_diagnostico,
      },
      });
    }
    if (!options.skipAlertRecalc) {
      await this.triggerAlertRecalculation('analisis-lubricante-create');
    }
    return this.wrap(
      {
        ...payload,
        requested_code: resolution.requestedCode,
        code_was_reassigned: resolution.codeWasReassigned,
        code_reassignment_reason: resolution.reassignmentReason,
      },
      'Análisis de lubricante creado',
    );
  }

  async updateAnalisisLubricante(
    id: string,
    dto: UpdateAnalisisLubricanteDto,
    options: AnalisisLubricanteSaveOptions = {},
  ) {
    await this.findOneOrFail(this.analisisLubricanteRepo, { id, is_deleted: false });

    await this.dataSource.transaction(async (manager) => {
      const analisisRepo = manager.getRepository(AnalisisLubricanteEntity);
      const detalleRepo = manager.getRepository(AnalisisLubricanteDetalleEntity);
      const row = await analisisRepo.findOne({ where: { id, is_deleted: false } });
      if (!row) throw new NotFoundException('Análisis de lubricante no encontrado');
      const mergedPayload = {
        ...((row.payload_json ?? {}) as Record<string, unknown>),
        ...((dto.payload_json ?? {}) as Record<string, unknown>),
      };
      const equipmentContext = await this.resolveAnalisisEquipmentContext(
        dto.equipo_id ?? row.equipo_id ?? null,
      );
      const mergedSampleInfo = this.extractAnalysisSampleInfo(mergedPayload);
      const payloadWithSampleInfo = this.mergeLubricantSampleInfo(mergedPayload, {
        ...mergedSampleInfo,
        equipo_marca: equipmentContext.marcaNombre ?? mergedSampleInfo.equipo_marca,
      });
      const lubricanteIdentity = this.resolveLubricantIdentity({
        lubricante: dto.lubricante ?? row.lubricante ?? null,
        marca_lubricante: dto.marca_lubricante ?? row.marca_lubricante ?? null,
        equipo_codigo: equipmentContext.equipo?.codigo ?? dto.equipo_codigo ?? row.equipo_codigo ?? null,
        equipo_nombre: equipmentContext.equipo?.nombre ?? dto.equipo_nombre ?? row.equipo_nombre ?? null,
        payload_json: payloadWithSampleInfo,
      });
      const preparedDetalles = dto.detalles
        ? await this.prepareAnalisisDetallesForSave({
            analisisId: row.id,
            lubricante: lubricanteIdentity.lubricante,
            compartimento:
              dto.compartimento_principal ?? row.compartimento_principal ?? null,
            detalles: dto.detalles,
          })
        : null;

      const resolvedState =
        dto.estado_diagnostico ??
        (preparedDetalles
          ? this.inferAnalisisStateFromDetails(
              preparedDetalles,
              mergedSampleInfo.condicion ?? row.estado_diagnostico,
            )
          : this.normalizeLubricantCondition(
              mergedSampleInfo.condicion ?? row.estado_diagnostico,
            ));
      const finalPayloadJson = this.mergeLubricantSampleInfo(payloadWithSampleInfo, {
        ...mergedSampleInfo,
        condicion: dto.estado_diagnostico
          ? this.normalizeLubricantCondition(dto.estado_diagnostico)
          : mergedSampleInfo.condicion,
        equipo_marca: equipmentContext.marcaNombre ?? mergedSampleInfo.equipo_marca,
      });

      Object.assign(row, {
        codigo: row.codigo,
        cliente: dto.cliente ?? row.cliente ?? null,
        equipo_id: dto.equipo_id ?? row.equipo_id ?? null,
        lubricante: lubricanteIdentity.lubricante,
        marca_lubricante: lubricanteIdentity.marca_lubricante,
        equipo_codigo:
          equipmentContext.equipo?.codigo ?? dto.equipo_codigo ?? row.equipo_codigo ?? null,
        equipo_nombre:
          equipmentContext.equipo?.nombre ?? dto.equipo_nombre ?? row.equipo_nombre ?? null,
        compartimento_principal:
          dto.compartimento_principal ?? row.compartimento_principal ?? null,
        fecha_muestra: dto.fecha_muestra
          ? this.toDateOnlyString(dto.fecha_muestra)
          : row.fecha_muestra ?? null,
        fecha_reporte: dto.fecha_reporte
          ? this.toDateOnlyString(dto.fecha_reporte)
          : row.fecha_reporte ?? null,
        diagnostico:
          String(dto.diagnostico ?? '').trim() ||
          this.buildLubricantDiagnosticText(
            preparedDetalles ?? [],
            resolvedState,
          ),
        estado_diagnostico: resolvedState,
        documento_origen: dto.documento_origen ?? row.documento_origen ?? null,
        payload_json: Object.assign(finalPayloadJson, {
          lubricante: lubricanteIdentity.lubricante,
          marca_lubricante: lubricanteIdentity.marca_lubricante,
        }),
      });
      await analisisRepo.save(row);

      if (preparedDetalles) {
        const current = await detalleRepo.find({
          where: { analisis_id: row.id, is_deleted: false },
        });
        for (const item of current) item.is_deleted = true;
        if (current.length) await detalleRepo.save(current);

        if (preparedDetalles.length) {
          await detalleRepo.save(
            preparedDetalles.map((detalle, index) =>
              detalleRepo.create({
                analisis_id: row.id,
                compartimento: detalle.compartimento,
                numero_muestra: detalle.numero_muestra ?? null,
                parametro: detalle.parametro,
                resultado_numerico: detalle.resultado_numerico ?? null,
                resultado_texto: detalle.resultado_texto ?? null,
                unidad: detalle.unidad ?? null,
                linea_base: detalle.linea_base ?? null,
                nivel_alerta: detalle.nivel_alerta ?? 'NORMAL',
                tendencia: detalle.tendencia ?? null,
                observacion: detalle.observacion ?? null,
                orden: detalle.orden ?? index + 1,
              }),
            ),
          );
        }
      }
    });

    const saved = await this.findOneOrFail(this.analisisLubricanteRepo, { id, is_deleted: false });
    const payload = await this.buildAnalisisLubricantePayload(saved);
    if (!options.skipProcessEvent) {
      await this.registerProcessEvent({
      tipo_proceso: 'ANALISIS_LUBRICANTE',
      accion: 'UPDATED',
      referencia_tabla: 'tb_analisis_lubricante',
      referencia_id: saved.id,
      referencia_codigo: saved.codigo,
      equipo_id: saved.equipo_id ?? null,
      title: 'Análisis de lubricante actualizado',
      body: `${saved.codigo}${saved.lubricante ? ` · ${saved.lubricante}` : ''}`,
      level:
        ['ANORMAL', 'PRECAUCION'].includes(
          this.normalizeLubricantCondition(saved.estado_diagnostico),
        )
          ? 'warning'
          : 'info',
      payload_kpi: {
        detalles: payload.detalles.length,
        estado: saved.estado_diagnostico,
      },
      });
    }
    if (!options.skipAlertRecalc) {
      await this.triggerAlertRecalculation('analisis-lubricante-update');
    }
    return this.wrap(payload, 'Análisis de lubricante actualizado');
  }

  async deleteAnalisisLubricante(id: string) {
    const row = await this.findOneOrFail(this.analisisLubricanteRepo, {
      id,
      is_deleted: false,
    });
    row.is_deleted = true;
    await this.analisisLubricanteRepo.save(row);
    await this.softDeleteRows(this.analisisLubricanteDetRepo, {
      analisis_id: row.id,
      is_deleted: false,
    });
    await this.triggerAlertRecalculation('analisis-lubricante-delete');
    return this.wrap(true, 'Análisis de lubricante eliminado');
  }

  async purgeAnalisisLubricante(dto: PurgeAnalisisLubricanteDto) {
    const confirmation = String(dto.confirmation ?? '').trim().toUpperCase();
    if (confirmation !== 'ELIMINAR TODO') {
      throw new BadRequestException(
        'Debes confirmar la operación escribiendo exactamente ELIMINAR TODO.',
      );
    }

    const requestedRole = String(dto.requested_role ?? '').trim().toUpperCase();
    if (!requestedRole || !requestedRole.includes('ADMIN')) {
      throw new BadRequestException(
        'Solo administradores pueden ejecutar el borrado total de análisis de lubricante.',
      );
    }

    const purgeImportJobs = dto.purge_import_jobs !== false;

    const [totalAnalyses, totalDetails, totalEvents, totalAlerts] =
      await Promise.all([
        this.analisisLubricanteRepo.count(),
        this.analisisLubricanteDetRepo.count(),
        this.eventoProcesoRepo.count({
          where: { referencia_tabla: 'tb_analisis_lubricante' },
        }),
        this.alertaRepo
          .createQueryBuilder('alerta')
          .where('alerta.origen = :origen', {
            origen: 'ANALISIS_LUBRICANTE',
          })
          .orWhere('alerta.referencia_tipo = :tipo', {
            tipo: 'ANALISIS_LUBRICANTE',
          })
          .orWhere('alerta.referencia LIKE :referencia', {
            referencia: 'ANALISIS:%',
          })
          .getCount(),
      ]);

    let deletedImportJobs = 0;
    if (purgeImportJobs) {
      try {
        const entries = await readdir(this.lubricantImportRoot, {
          withFileTypes: true,
        });
        deletedImportJobs = entries.filter((entry) => entry.isDirectory()).length;
      } catch {
        deletedImportJobs = 0;
      }
    }

    await this.dataSource.transaction(async (manager) => {
      await manager
        .createQueryBuilder()
        .delete()
        .from(AnalisisLubricanteDetalleEntity)
        .execute();

      await manager
        .createQueryBuilder()
        .delete()
        .from(AnalisisLubricanteEntity)
        .execute();

      await manager
        .createQueryBuilder()
        .delete()
        .from(EventoProcesoEntity)
        .where('referencia_tabla = :tabla', {
          tabla: 'tb_analisis_lubricante',
        })
        .execute();

      await manager
        .createQueryBuilder()
        .delete()
        .from(AlertaMantenimientoEntity)
        .where('origen = :origen', { origen: 'ANALISIS_LUBRICANTE' })
        .orWhere('referencia_tipo = :tipo', {
          tipo: 'ANALISIS_LUBRICANTE',
        })
        .orWhere('referencia LIKE :referencia', {
          referencia: 'ANALISIS:%',
        })
        .execute();
    });

    if (purgeImportJobs) {
      await rm(this.lubricantImportRoot, { recursive: true, force: true });
      await mkdir(this.lubricantImportRoot, { recursive: true });
      this.lubricantImportJobs.clear();
    }

    await this.writeSecurityLog({
      typeLog: 'LUBRICANT_PURGE_ALL',
      description: `Purga total de análisis de lubricante ejecutada. Analisis=${totalAnalyses}, detalles=${totalDetails}, eventos=${totalEvents}, alertas=${totalAlerts}, importaciones=${deletedImportJobs}`,
      createdBy: dto.requested_by ?? null,
    });

    await this.triggerAlertRecalculation('analisis-lubricante-purge');

    return this.wrap(
      {
        deleted_analyses: totalAnalyses,
        deleted_details: totalDetails,
        deleted_events: totalEvents,
        deleted_alerts: totalAlerts,
        deleted_import_jobs: purgeImportJobs ? deletedImportJobs : 0,
        purge_import_jobs: purgeImportJobs,
      },
      'Toda la información de análisis de lubricante fue eliminada físicamente',
    );
  }

  private async importAnalisisLubricanteItems(
    items: CreateAnalisisLubricanteDto[],
    options?: {
      upsertExisting?: boolean;
      onProgress?: (payload: {
        index: number;
        total: number;
        action: 'created' | 'updated' | 'error';
        message?: string;
      }) => Promise<void> | void;
    },
  ) {
    const sanitizedItems = Array.isArray(items)
      ? items.filter((item) => item && typeof item === 'object')
      : [];
    if (!sanitizedItems.length) {
      throw new BadRequestException(
        'No se recibieron análisis válidos para importar.',
      );
    }

    const existingRows = await this.analisisLubricanteRepo.find({
      where: { is_deleted: false },
      order: { created_at: 'DESC' },
    });

    const existingMap = new Map<string, AnalisisLubricanteEntity>();
    for (const row of existingRows) {
      const identity = this.buildAnalisisImportIdentity({
        equipo_id: row.equipo_id ?? null,
        equipo_codigo: row.equipo_codigo ?? null,
        compartimento_principal: row.compartimento_principal ?? null,
        fecha_muestra: row.fecha_muestra ?? null,
        lubricante: row.lubricante ?? null,
        payload_json: (row.payload_json ?? {}) as Record<string, unknown>,
      });
      if (identity) {
        existingMap.set(identity, row);
      }
    }

    const summary = {
      total: sanitizedItems.length,
      created: 0,
      updated: 0,
      skipped: 0,
      errors: [] as Array<{ index: number; message: string }>,
      imported_ids: [] as string[],
    };

    for (let index = 0; index < sanitizedItems.length; index += 1) {
      const item = sanitizedItems[index];
      try {
        const identity = this.buildAnalisisImportIdentity({
          equipo_id: item.equipo_id ?? null,
          equipo_codigo: item.equipo_codigo ?? null,
          compartimento_principal: item.compartimento_principal ?? null,
          fecha_muestra: item.fecha_muestra ?? null,
          lubricante: item.lubricante ?? null,
          payload_json: (item.payload_json ?? {}) as Record<string, unknown>,
        });

        const existing =
          identity && options?.upsertExisting !== false
            ? existingMap.get(identity) ?? null
            : null;

        if (existing) {
          const updated = (await this.updateAnalisisLubricante(
            existing.id,
            {
              ...item,
              codigo: existing.codigo,
            },
            {
              skipAlertRecalc: true,
              skipProcessEvent: true,
            },
          )) as any;
          summary.updated += 1;
          if (updated?.data?.id) {
            summary.imported_ids.push(String(updated.data.id));
          } else {
            summary.imported_ids.push(existing.id);
          }
          await options?.onProgress?.({
            index: index + 1,
            total: sanitizedItems.length,
            action: 'updated',
          });
          continue;
        }

        const created = (await this.createAnalisisLubricante(item, {
          skipAlertRecalc: true,
          skipProcessEvent: true,
        })) as any;
        summary.created += 1;
        if (created?.data?.id) {
          summary.imported_ids.push(String(created.data.id));
          const createdRow = created.data as AnalisisLubricanteEntity & {
            payload_json?: Record<string, unknown>;
          };
          const createdIdentity = this.buildAnalisisImportIdentity({
            equipo_id: createdRow.equipo_id ?? null,
            equipo_codigo: createdRow.equipo_codigo ?? null,
            compartimento_principal: createdRow.compartimento_principal ?? null,
            fecha_muestra: createdRow.fecha_muestra ?? null,
            lubricante: createdRow.lubricante ?? null,
            payload_json: createdRow.payload_json ?? {},
          });
          if (createdIdentity) {
            existingMap.set(createdIdentity, createdRow);
          }
        }
        await options?.onProgress?.({
          index: index + 1,
          total: sanitizedItems.length,
          action: 'created',
        });
      } catch (error: any) {
        summary.errors.push({
          index,
          message:
            error?.response?.data?.message ||
            error?.message ||
            'No se pudo importar el análisis.',
        });
      }
    }

    summary.skipped = summary.errors.length;

    if (summary.created > 0 || summary.updated > 0) {
      await this.triggerAlertRecalculation('analisis-lubricante-import');
    }

    return summary; /*
      `Importación de análisis de lubricante procesada desde ${
    */
  }

  private getLubricantImportJobDir(jobId: string) {
    return join(this.lubricantImportRoot, jobId);
  }

  private getLubricantImportStatusPath(jobId: string) {
    return join(this.getLubricantImportJobDir(jobId), 'status.json');
  }

  private getLubricantImportLogPath(jobId: string) {
    return join(this.getLubricantImportJobDir(jobId), 'import.log');
  }

  private buildLubricantImportJobResponse(job: LubricantImportJobState) {
    return {
      ...job,
      logs: [...job.logs].slice(-80),
    };
  }

  private async persistLubricantImportJob(job: LubricantImportJobState) {
    await mkdir(this.getLubricantImportJobDir(job.id), { recursive: true });
    await writeFile(
      this.getLubricantImportStatusPath(job.id),
      JSON.stringify(this.buildLubricantImportJobResponse(job), null, 2),
      'utf8',
    );
  }

  private async loadLubricantImportJob(jobId: string) {
    const inMemory = this.lubricantImportJobs.get(jobId);
    if (inMemory) return inMemory;
    try {
      const raw = await readFile(this.getLubricantImportStatusPath(jobId), 'utf8');
      const parsed = JSON.parse(raw) as LubricantImportJobState;
      this.lubricantImportJobs.set(jobId, parsed);
      return parsed;
    } catch {
      throw new NotFoundException('Importacion de lubricante no encontrada');
    }
  }

  private async appendLubricantImportLog(
    job: LubricantImportJobState,
    level: LubricantImportLogLevel,
    message: string,
    context?: Record<string, unknown> | null,
  ) {
    const entry: LubricantImportLogEntry = {
      at: new Date().toISOString(),
      level,
      message,
      context: context ?? null,
    };
    job.logs.push(entry);
    if (job.logs.length > 200) {
      job.logs = job.logs.slice(-200);
    }
    await mkdir(this.getLubricantImportJobDir(job.id), { recursive: true });
    await appendFile(
      this.getLubricantImportLogPath(job.id),
      `${entry.at} [${entry.level}] ${entry.message}${
        context ? ` ${JSON.stringify(context)}` : ''
      }\n`,
      'utf8',
    );
    await this.persistLubricantImportJob(job);
  }

  async startAnalisisLubricanteImport(
    file: {
      originalname?: string;
      buffer?: Buffer;
      size?: number;
    } | null,
    options?: {
      upsert_existing?: unknown;
      requested_by?: string | null;
    },
  ) {
    if (!file?.buffer?.length) {
      throw new BadRequestException('Debes adjuntar un archivo Excel valido.');
    }
    const extension = extname(String(file.originalname ?? '')).toLowerCase();
    if (!['.xlsx', '.xls'].includes(extension)) {
      throw new BadRequestException(
        'El archivo debe tener formato .xlsx o .xls.',
      );
    }

    const jobId = randomUUID();
    const storedFileName = `source${extension || '.xlsx'}`;
    const storedFilePath = join(
      this.getLubricantImportJobDir(jobId),
      storedFileName,
    );
    await mkdir(this.getLubricantImportJobDir(jobId), { recursive: true });
    await writeFile(storedFilePath, file.buffer);

    const job: LubricantImportJobState = {
      id: jobId,
      status: 'QUEUED',
      progress: 0,
      current_step: 'Archivo recibido',
      current_index: 0,
      total_steps: 0,
      upsert_existing: this.coerceBoolean(options?.upsert_existing, true),
      requested_by: String(options?.requested_by ?? '').trim() || null,
      created_at: new Date().toISOString(),
      started_at: null,
      finished_at: null,
      source_file_name: String(file.originalname ?? storedFileName),
      stored_file_name: storedFileName,
      stored_file_path: storedFilePath,
      file_size_bytes: Number(file.size ?? file.buffer.length ?? 0),
      warnings: [],
      errors: [],
      logs: [],
      summary: {
        total: 0,
        created: 0,
        updated: 0,
        skipped: 0,
        imported_ids: [],
      },
      error_message: null,
    };

    this.lubricantImportJobs.set(job.id, job);
    await this.persistLubricantImportJob(job);
    await this.appendLubricantImportLog(
      job,
      'INFO',
      'Archivo cargado en servidor y encolado para procesamiento.',
      {
        file_name: job.source_file_name,
        size_bytes: job.file_size_bytes,
      },
    );
    await this.writeSecurityLog({
      typeLog: 'LUBRICANT_IMPORT_START',
      description: `Inicio de importacion de lubricante ${job.id} desde ${job.source_file_name}`,
      createdBy: job.requested_by,
    });

    setImmediate(() => {
      void this.runAnalisisLubricanteImport(job.id);
    });

    return this.wrap(
      this.buildLubricantImportJobResponse(job),
      'Archivo de lubricante recibido para procesamiento en segundo plano',
    );
  }

  async getAnalisisLubricanteImportStatus(jobId: string) {
    const job = await this.loadLubricantImportJob(jobId);
    return this.wrap(
      this.buildLubricantImportJobResponse(job),
      'Estado de importacion de lubricante obtenido',
    );
  }

  private async runAnalisisLubricanteImport(jobId: string) {
    const job = await this.loadLubricantImportJob(jobId);
    try {
      job.status = 'PARSING';
      job.started_at = new Date().toISOString();
      job.progress = 5;
      job.current_step = 'Leyendo workbook';
      await this.persistLubricantImportJob(job);
      await this.appendLubricantImportLog(
        job,
        'INFO',
        'Inicio de lectura y validacion del workbook.',
      );

      const buffer = await readFile(job.stored_file_path);
      const parsed = await this.parseLubricantWorkbook(
        buffer,
        job.source_file_name,
      );

      job.warnings = parsed.warnings;
      job.total_steps = parsed.analyses.length;
      job.progress = parsed.analyses.length ? 10 : 100;
      job.current_step = parsed.analyses.length
        ? 'Procesando muestras'
        : 'Sin muestras validas';
      await this.persistLubricantImportJob(job);
      for (const warning of parsed.warnings) {
        await this.appendLubricantImportLog(job, 'WARNING', warning);
      }

      if (!parsed.analyses.length) {
        throw new BadRequestException(
          'El archivo no contiene muestras validas para importar.',
        );
      }

      job.status = 'PROCESSING';
      await this.persistLubricantImportJob(job);

      const summary = await this.importAnalisisLubricanteItems(parsed.analyses, {
        upsertExisting: job.upsert_existing,
        onProgress: async (payload) => {
          job.current_index = payload.index;
          job.total_steps = payload.total;
          job.current_step = `Procesando muestra ${payload.index} de ${payload.total}`;
          job.progress = Math.max(
            10,
            Math.min(
              99,
              Math.round(10 + (payload.index / Math.max(payload.total, 1)) * 85),
            ),
          );
          if (payload.index === 1 || payload.index % 10 === 0) {
            await this.appendLubricantImportLog(
              job,
              payload.action === 'error' ? 'ERROR' : 'INFO',
              `Avance ${payload.index}/${payload.total}`,
              {
                action: payload.action,
                message: payload.message ?? null,
              },
            );
          } else {
            await this.persistLubricantImportJob(job);
          }
        },
      });

      job.summary = summary;
      job.errors = summary.errors;
      job.status = 'COMPLETED';
      job.progress = 100;
      job.current_step = 'Importacion finalizada';
      job.finished_at = new Date().toISOString();
      await this.persistLubricantImportJob(job);
      await this.appendLubricantImportLog(
        job,
        summary.errors.length ? 'WARNING' : 'INFO',
        'Importacion de lubricante completada.',
        {
          created: summary.created,
          updated: summary.updated,
          errors: summary.errors.length,
        },
      );
      await this.writeSecurityLog({
        typeLog: 'LUBRICANT_IMPORT_SUCCESS',
        description: `Importacion de lubricante ${job.id} finalizada. Creados=${summary.created}, actualizados=${summary.updated}, errores=${summary.errors.length}`,
        createdBy: job.requested_by,
      });
    } catch (error: any) {
      job.status = 'FAILED';
      job.finished_at = new Date().toISOString();
      job.current_step = 'Importacion fallida';
      job.error_message =
        error?.message || 'Ocurrio un error durante la importacion.';
      await this.persistLubricantImportJob(job);
      await this.appendLubricantImportLog(
        job,
        'ERROR',
        'La importacion de lubricante fallo.',
        { message: job.error_message },
      );
      await this.writeSecurityLog({
        typeLog: 'LUBRICANT_IMPORT_ERROR',
        description: `Importacion de lubricante ${job.id} fallo: ${job.error_message}`,
        status: 'ERROR',
        createdBy: job.requested_by,
      });
      this.logger.error(
        `Error en importacion de lubricante ${job.id}: ${job.error_message}`,
      );
    }
  }

  async importAnalisisLubricanteBatch(dto: ImportAnalisisLubricanteBatchDto) {
    const summary = await this.importAnalisisLubricanteItems(dto.analyses, {
      upsertExisting: dto.upsert_existing !== false,
    });
    return this.wrap(
      summary,
      `Importacion de analisis de lubricante procesada desde ${
        dto.source_file_name || 'archivo'
      }`,
    );
  }

  async listCronogramasSemanales() {
    const rows = await this.cronogramaSemanalRepo.find({
      where: { is_deleted: false },
      order: { fecha_inicio: 'DESC', created_at: 'DESC' },
    });
    return this.wrap(
      await Promise.all(rows.map((row) => this.buildCronogramaSemanalPayload(row))),
      'Cronogramas semanales listados',
    );
  }

  async getCronogramaSemanal(id: string) {
    const row = await this.findOneOrFail(this.cronogramaSemanalRepo, {
      id,
      is_deleted: false,
    });
    return this.wrap(
      await this.buildCronogramaSemanalPayload(row),
      'Cronograma semanal obtenido',
    );
  }

  async createCronogramaSemanal(dto: CreateCronogramaSemanalDto) {
    const savedId = await this.dataSource.transaction(async (manager) => {
      const cronogramaRepo = manager.getRepository(CronogramaSemanalEntity);
      const detalleRepo = manager.getRepository(CronogramaSemanalDetalleEntity);
      const row = cronogramaRepo.create();
      Object.assign(row, {
        codigo: dto.codigo,
        fecha_inicio: dto.fecha_inicio.slice(0, 10),
        fecha_fin: dto.fecha_fin.slice(0, 10),
        locacion: dto.locacion ?? null,
        referencia_orden: dto.referencia_orden ?? null,
        documento_origen: dto.documento_origen ?? null,
        resumen: dto.resumen ?? null,
        payload_json: dto.payload_json ?? {},
      });
      await cronogramaRepo.save(row);

      if (dto.detalles?.length) {
        await detalleRepo.save(
          dto.detalles.map((detalle, index) =>
            detalleRepo.create({
              cronograma_id: row.id,
              dia_semana: detalle.dia_semana,
              fecha_actividad: this.toDateOnlyString(detalle.fecha_actividad),
              hora_inicio: this.toTimeOnlyString(detalle.hora_inicio),
              hora_fin: this.toTimeOnlyString(detalle.hora_fin),
              tipo_proceso: detalle.tipo_proceso ?? null,
              actividad: detalle.actividad,
              responsable_area: detalle.responsable_area ?? null,
              equipo_codigo: detalle.equipo_codigo ?? null,
              observacion: detalle.observacion ?? null,
              orden: detalle.orden ?? index + 1,
            }),
          ),
        );
      }

      return row.id;
    });

    const saved = await this.findOneOrFail(this.cronogramaSemanalRepo, {
      id: savedId,
      is_deleted: false,
    });
    const payload = await this.buildCronogramaSemanalPayload(saved);
    await this.registerProcessEvent({
      tipo_proceso: 'CRONOGRAMA_SEMANAL',
      accion: 'CREATED',
      referencia_tabla: 'tb_cronograma_semanal',
      referencia_id: saved.id,
      referencia_codigo: saved.codigo,
      title: 'Nuevo cronograma semanal',
      body: `${saved.codigo} (${saved.fecha_inicio} - ${saved.fecha_fin})`,
      payload_kpi: {
        actividades: payload.detalles.length,
      },
    });
    return this.wrap(payload, 'Cronograma semanal creado');
  }

  async updateCronogramaSemanal(id: string, dto: UpdateCronogramaSemanalDto) {
    await this.findOneOrFail(this.cronogramaSemanalRepo, { id, is_deleted: false });

    await this.dataSource.transaction(async (manager) => {
      const cronogramaRepo = manager.getRepository(CronogramaSemanalEntity);
      const detalleRepo = manager.getRepository(CronogramaSemanalDetalleEntity);
      const row = await cronogramaRepo.findOne({ where: { id, is_deleted: false } });
      if (!row) throw new NotFoundException('Cronograma semanal no encontrado');

      Object.assign(row, {
        codigo: dto.codigo ?? row.codigo,
        fecha_inicio: dto.fecha_inicio ? this.toDateOnlyString(dto.fecha_inicio) : row.fecha_inicio,
        fecha_fin: dto.fecha_fin ? this.toDateOnlyString(dto.fecha_fin) : row.fecha_fin,
        locacion: dto.locacion ?? row.locacion ?? null,
        referencia_orden: dto.referencia_orden ?? row.referencia_orden ?? null,
        documento_origen: dto.documento_origen ?? row.documento_origen ?? null,
        resumen: dto.resumen ?? row.resumen ?? null,
        payload_json: dto.payload_json ?? row.payload_json ?? {},
      });
      await cronogramaRepo.save(row);

      if (dto.detalles) {
        const current = await detalleRepo.find({
          where: { cronograma_id: row.id, is_deleted: false },
        });
        for (const item of current) item.is_deleted = true;
        if (current.length) await detalleRepo.save(current);

        if (dto.detalles.length) {
          await detalleRepo.save(
            dto.detalles.map((detalle, index) =>
              detalleRepo.create({
                cronograma_id: row.id,
                dia_semana: detalle.dia_semana,
                fecha_actividad: this.toDateOnlyString(detalle.fecha_actividad),
                hora_inicio: this.toTimeOnlyString(detalle.hora_inicio),
                hora_fin: this.toTimeOnlyString(detalle.hora_fin),
                tipo_proceso: detalle.tipo_proceso ?? null,
                actividad: detalle.actividad,
                responsable_area: detalle.responsable_area ?? null,
                equipo_codigo: detalle.equipo_codigo ?? null,
                observacion: detalle.observacion ?? null,
                orden: detalle.orden ?? index + 1,
              }),
            ),
          );
        }
      }
    });

    const saved = await this.findOneOrFail(this.cronogramaSemanalRepo, { id, is_deleted: false });
    const payload = await this.buildCronogramaSemanalPayload(saved);
    await this.registerProcessEvent({
      tipo_proceso: 'CRONOGRAMA_SEMANAL',
      accion: 'UPDATED',
      referencia_tabla: 'tb_cronograma_semanal',
      referencia_id: saved.id,
      referencia_codigo: saved.codigo,
      title: 'Cronograma semanal actualizado',
      body: `${saved.codigo} (${saved.fecha_inicio} - ${saved.fecha_fin})`,
      payload_kpi: {
        actividades: payload.detalles.length,
      },
    });
    return this.wrap(payload, 'Cronograma semanal actualizado');
  }

  async deleteCronogramaSemanal(id: string) {
    const row = await this.findOneOrFail(this.cronogramaSemanalRepo, {
      id,
      is_deleted: false,
    });
    row.is_deleted = true;
    await this.cronogramaSemanalRepo.save(row);
    await this.softDeleteRows(this.cronogramaSemanalDetRepo, {
      cronograma_id: row.id,
      is_deleted: false,
    });
    return this.wrap(true, 'Cronograma semanal eliminado');
  }

  async listReportesOperacionDiaria() {
    const rows = await this.reporteDiarioRepo.find({
      where: { is_deleted: false },
      order: { fecha_reporte: 'DESC', created_at: 'DESC' },
    });
    return this.wrap(
      await Promise.all(rows.map((row) => this.buildReporteDiarioPayload(row))),
      'Reportes de operación diaria listados',
    );
  }

  async getReporteOperacionDiaria(id: string) {
    const row = await this.findOneOrFail(this.reporteDiarioRepo, {
      id,
      is_deleted: false,
    });
    return this.wrap(
      await this.buildReporteDiarioPayload(row),
      'Reporte de operación diaria obtenido',
    );
  }

  async createReporteOperacionDiaria(dto: CreateReporteOperacionDiariaDto) {
    const savedId = await this.dataSource.transaction(async (manager) => {
      const reporteRepo = manager.getRepository(ReporteOperacionDiariaEntity);
      const unidadRepo = manager.getRepository(ReporteOperacionDiariaUnidadEntity);
      const combustibleRepo = manager.getRepository(ReporteCombustibleEntity);
      const componenteRepo = manager.getRepository(ControlComponenteEntity);

      const row = reporteRepo.create();
      Object.assign(row, {
        codigo: dto.codigo,
        fecha_reporte: dto.fecha_reporte.slice(0, 10),
        locacion: dto.locacion ?? null,
        turno: dto.turno ?? null,
        documento_origen: dto.documento_origen ?? null,
        resumen: dto.resumen ?? null,
        payload_json: dto.payload_json ?? {},
      });
      await reporteRepo.save(row);

      if (dto.unidades?.length) {
        await unidadRepo.save(
          dto.unidades.map((unidad) =>
            unidadRepo.create({
              reporte_id: row.id,
              equipo_id: unidad.equipo_id ?? null,
              equipo_codigo: unidad.equipo_codigo,
              fabricante: unidad.fabricante ?? null,
              modo_operacion: unidad.modo_operacion ?? null,
              carga_kw: unidad.carga_kw ?? null,
              horometro_actual: unidad.horometro_actual ?? null,
              horometro_inicio: unidad.horometro_inicio ?? null,
              horas_operacion: unidad.horas_operacion ?? null,
              mpg_actual: unidad.mpg_actual ?? null,
              proximo_mpg: unidad.proximo_mpg ?? null,
              horas_faltantes: unidad.horas_faltantes ?? null,
              dias_faltantes: unidad.dias_faltantes ?? null,
              fecha_proxima: this.toDateOnlyString(unidad.fecha_proxima),
              nota: unidad.nota ?? null,
            }),
          ),
        );
      }

      if (dto.combustibles?.length) {
        await combustibleRepo.save(
          dto.combustibles.map((combustible) =>
            combustibleRepo.create({
              reporte_id: row.id,
              tanque: combustible.tanque,
              tipo_lectura: combustible.tipo_lectura ?? 'STOCK',
              fecha_lectura: combustible.fecha_lectura
                ? new Date(combustible.fecha_lectura)
                : new Date(),
              medida_cm: combustible.medida_cm ?? null,
              medida_ft: combustible.medida_ft ?? null,
              medida_in: combustible.medida_in ?? null,
              galones: combustible.galones ?? null,
              stock_anterior: combustible.stock_anterior ?? null,
              stock_actual: combustible.stock_actual ?? null,
              stock_minimo: combustible.stock_minimo ?? null,
              stock_maximo: combustible.stock_maximo ?? null,
              consumo_galones: combustible.consumo_galones ?? null,
              guia_remision: combustible.guia_remision ?? null,
              observacion: combustible.observacion ?? null,
            }),
          ),
        );
      }

      if (dto.componentes?.length) {
        await componenteRepo.save(
          dto.componentes.map((componente) =>
            componenteRepo.create({
              reporte_id: row.id,
              equipo_id: componente.equipo_id ?? null,
              equipo_codigo: componente.equipo_codigo,
              tipo_componente: componente.tipo_componente,
              posicion: componente.posicion ?? null,
              serie: componente.serie ?? null,
              estado: componente.estado ?? null,
              fecha_instalacion: this.toDateOnlyString(componente.fecha_instalacion),
              fecha_retiro: this.toDateOnlyString(componente.fecha_retiro),
              horometro_instalacion: componente.horometro_instalacion ?? null,
              horometro_retiro: componente.horometro_retiro ?? null,
              horas_uso: componente.horas_uso ?? null,
              motivo: componente.motivo ?? null,
              responsable: componente.responsable ?? null,
              documento_origen: componente.documento_origen ?? null,
              meta: componente.meta ?? {},
            }),
          ),
        );
      }

      return row.id;
    });

    const saved = await this.findOneOrFail(this.reporteDiarioRepo, {
      id: savedId,
      is_deleted: false,
    });
    const payload = await this.buildReporteDiarioPayload(saved);
    await this.registerProcessEvent({
      tipo_proceso: 'REPORTE_OPERACION_DIARIA',
      accion: 'CREATED',
      referencia_tabla: 'tb_reporte_operacion_diaria',
      referencia_id: saved.id,
      referencia_codigo: saved.codigo,
      title: 'Nuevo reporte de operación diaria',
      body: `${saved.codigo} · ${saved.fecha_reporte}`,
      payload_kpi: {
        unidades: payload.unidades.length,
        combustibles: payload.combustibles.length,
        componentes: payload.componentes.length,
      },
    });
    await this.triggerAlertRecalculation('reporte-diario-create');
    return this.wrap(payload, 'Reporte de operación diaria creado');
  }

  async updateReporteOperacionDiaria(id: string, dto: UpdateReporteOperacionDiariaDto) {
    await this.findOneOrFail(this.reporteDiarioRepo, { id, is_deleted: false });

    await this.dataSource.transaction(async (manager) => {
      const reporteRepo = manager.getRepository(ReporteOperacionDiariaEntity);
      const unidadRepo = manager.getRepository(ReporteOperacionDiariaUnidadEntity);
      const combustibleRepo = manager.getRepository(ReporteCombustibleEntity);
      const componenteRepo = manager.getRepository(ControlComponenteEntity);
      const row = await reporteRepo.findOne({ where: { id, is_deleted: false } });
      if (!row) throw new NotFoundException('Reporte de operación diaria no encontrado');

      Object.assign(row, {
        codigo: dto.codigo ?? row.codigo,
        fecha_reporte: dto.fecha_reporte
          ? this.toDateOnlyString(dto.fecha_reporte)
          : row.fecha_reporte,
        locacion: dto.locacion ?? row.locacion ?? null,
        turno: dto.turno ?? row.turno ?? null,
        documento_origen: dto.documento_origen ?? row.documento_origen ?? null,
        resumen: dto.resumen ?? row.resumen ?? null,
        payload_json: dto.payload_json ?? row.payload_json ?? {},
      });
      await reporteRepo.save(row);

      if (dto.unidades) {
        const current = await unidadRepo.find({
          where: { reporte_id: row.id, is_deleted: false },
        });
        for (const item of current) item.is_deleted = true;
        if (current.length) await unidadRepo.save(current);
        if (dto.unidades.length) {
          await unidadRepo.save(
            dto.unidades.map((unidad) =>
              unidadRepo.create({
                reporte_id: row.id,
                equipo_id: unidad.equipo_id ?? null,
                equipo_codigo: unidad.equipo_codigo,
                fabricante: unidad.fabricante ?? null,
                modo_operacion: unidad.modo_operacion ?? null,
                carga_kw: unidad.carga_kw ?? null,
                horometro_actual: unidad.horometro_actual ?? null,
                horometro_inicio: unidad.horometro_inicio ?? null,
                horas_operacion: unidad.horas_operacion ?? null,
                mpg_actual: unidad.mpg_actual ?? null,
                proximo_mpg: unidad.proximo_mpg ?? null,
                horas_faltantes: unidad.horas_faltantes ?? null,
                dias_faltantes: unidad.dias_faltantes ?? null,
                fecha_proxima: this.toDateOnlyString(unidad.fecha_proxima),
                nota: unidad.nota ?? null,
              }),
            ),
          );
        }
      }

      if (dto.combustibles) {
        const current = await combustibleRepo.find({
          where: { reporte_id: row.id, is_deleted: false },
        });
        for (const item of current) item.is_deleted = true;
        if (current.length) await combustibleRepo.save(current);
        if (dto.combustibles.length) {
          await combustibleRepo.save(
            dto.combustibles.map((combustible) =>
              combustibleRepo.create({
                reporte_id: row.id,
                tanque: combustible.tanque,
                tipo_lectura: combustible.tipo_lectura ?? 'STOCK',
                fecha_lectura: combustible.fecha_lectura
                  ? new Date(combustible.fecha_lectura)
                  : new Date(),
                medida_cm: combustible.medida_cm ?? null,
                medida_ft: combustible.medida_ft ?? null,
                medida_in: combustible.medida_in ?? null,
                galones: combustible.galones ?? null,
                stock_anterior: combustible.stock_anterior ?? null,
                stock_actual: combustible.stock_actual ?? null,
                stock_minimo: combustible.stock_minimo ?? null,
                stock_maximo: combustible.stock_maximo ?? null,
                consumo_galones: combustible.consumo_galones ?? null,
                guia_remision: combustible.guia_remision ?? null,
                observacion: combustible.observacion ?? null,
              }),
            ),
          );
        }
      }

      if (dto.componentes) {
        const current = await componenteRepo.find({
          where: { reporte_id: row.id, is_deleted: false },
        });
        for (const item of current) item.is_deleted = true;
        if (current.length) await componenteRepo.save(current);
        if (dto.componentes.length) {
          await componenteRepo.save(
            dto.componentes.map((componente) =>
              componenteRepo.create({
                reporte_id: row.id,
                equipo_id: componente.equipo_id ?? null,
                equipo_codigo: componente.equipo_codigo,
                tipo_componente: componente.tipo_componente,
                posicion: componente.posicion ?? null,
                serie: componente.serie ?? null,
                estado: componente.estado ?? null,
                fecha_instalacion: this.toDateOnlyString(componente.fecha_instalacion),
                fecha_retiro: this.toDateOnlyString(componente.fecha_retiro),
                horometro_instalacion: componente.horometro_instalacion ?? null,
                horometro_retiro: componente.horometro_retiro ?? null,
                horas_uso: componente.horas_uso ?? null,
                motivo: componente.motivo ?? null,
                responsable: componente.responsable ?? null,
                documento_origen: componente.documento_origen ?? null,
                meta: componente.meta ?? {},
              }),
            ),
          );
        }
      }
    });

    const saved = await this.findOneOrFail(this.reporteDiarioRepo, { id, is_deleted: false });
    const payload = await this.buildReporteDiarioPayload(saved);
    await this.registerProcessEvent({
      tipo_proceso: 'REPORTE_OPERACION_DIARIA',
      accion: 'UPDATED',
      referencia_tabla: 'tb_reporte_operacion_diaria',
      referencia_id: saved.id,
      referencia_codigo: saved.codigo,
      title: 'Reporte de operación diaria actualizado',
      body: `${saved.codigo} · ${saved.fecha_reporte}`,
      payload_kpi: {
        unidades: payload.unidades.length,
        combustibles: payload.combustibles.length,
        componentes: payload.componentes.length,
      },
    });
    await this.triggerAlertRecalculation('reporte-diario-update');
    return this.wrap(payload, 'Reporte de operación diaria actualizado');
  }

  async deleteReporteOperacionDiaria(id: string) {
    const row = await this.findOneOrFail(this.reporteDiarioRepo, {
      id,
      is_deleted: false,
    });
    row.is_deleted = true;
    await this.reporteDiarioRepo.save(row);
    await Promise.all([
      this.softDeleteRows(this.reporteDiarioUnidadRepo, {
        reporte_id: row.id,
        is_deleted: false,
      }),
      this.softDeleteRows(this.reporteCombustibleRepo, {
        reporte_id: row.id,
        is_deleted: false,
      }),
      this.softDeleteRows(this.controlComponenteRepo, {
        reporte_id: row.id,
        is_deleted: false,
      }),
    ]);
    await this.triggerAlertRecalculation('reporte-diario-delete');
    return this.wrap(true, 'Reporte de operación diaria eliminado');
  }

  async listEventosProceso(query: EventoProcesoQueryDto) {
    const where: FindOptionsWhere<EventoProcesoEntity> = { is_deleted: false };
    if (query.tipo_proceso) where.tipo_proceso = query.tipo_proceso;
    const rows = await this.eventoProcesoRepo.find({
      where,
      order: { fecha_evento: 'DESC', created_at: 'DESC' },
      take: Math.min(Math.max(query.limit ?? 20, 1), 100),
    });
    return this.wrap(rows, 'Eventos de proceso listados');
  }

  async listControlComponentesCriticos() {
    const rows = await this.controlComponenteRepo.find({
      where: { is_deleted: false },
      order: { updated_at: 'DESC', created_at: 'DESC' },
    });

    const reportIds = [...new Set(rows.map((row) => row.reporte_id).filter(Boolean))] as string[];
    const reportes = reportIds.length
      ? await this.reporteDiarioRepo.find({
          where: { id: In(reportIds), is_deleted: false },
        })
      : [];
    const reportMap = new Map(reportes.map((row) => [row.id, row]));

    return this.wrap(
      rows.map((row) => {
        const reporte = row.reporte_id ? reportMap.get(row.reporte_id) : null;
        return {
          ...row,
          reporte_codigo: reporte?.codigo ?? null,
          fecha_reporte: reporte?.fecha_reporte ?? null,
          turno_reporte: reporte?.turno ?? null,
          locacion_reporte: reporte?.locacion ?? null,
        };
      }),
      'Componentes criticos listados',
    );
  }

  async getIntelligenceSummary() {
    const [
      procedimientos,
      analisisTotal,
      cronogramasTotal,
      reportesTotal,
      eventosTotal,
      analisisRows,
      cronogramas,
      reportes,
      eventos,
      overdueProgramaciones,
      pendingWorkOrders,
      componentesTotal,
      componentesRecientes,
      allAnalysesForLubricants,
    ] = await Promise.all([
      this.procedimientoRepo.count({ where: { is_deleted: false } }),
      this.analisisLubricanteRepo.count({ where: { is_deleted: false } }),
      this.cronogramaSemanalRepo.count({ where: { is_deleted: false } }),
      this.reporteDiarioRepo.count({ where: { is_deleted: false } }),
      this.eventoProcesoRepo.count({ where: { is_deleted: false } }),
      this.analisisLubricanteRepo.find({
        where: { is_deleted: false },
        order: { fecha_reporte: 'DESC', created_at: 'DESC' },
        take: 10,
      }),
      this.cronogramaSemanalRepo.find({
        where: { is_deleted: false },
        order: { fecha_inicio: 'DESC', created_at: 'DESC' },
        take: 10,
      }),
      this.reporteDiarioRepo.find({
        where: { is_deleted: false },
        order: { fecha_reporte: 'DESC', created_at: 'DESC' },
        take: 10,
      }),
      this.eventoProcesoRepo.find({
        where: { is_deleted: false },
        order: { fecha_evento: 'DESC', created_at: 'DESC' },
        take: 20,
      }),
      this.programacionRepo.find({ where: { is_deleted: false, activo: true } }),
      this.woRepo.count({
        where: { is_deleted: false, status_workflow: In(['PLANNED', 'IN_PROGRESS']) },
      }),
      this.controlComponenteRepo.count({ where: { is_deleted: false } }),
      this.controlComponenteRepo.find({
        where: { is_deleted: false },
        order: { updated_at: 'DESC', created_at: 'DESC' },
        take: 10,
      }),
      this.analisisLubricanteRepo.find({
        where: { is_deleted: false },
      }),
    ]);

    const programaciones = await Promise.all(
      overdueProgramaciones.map((row) =>
        this.recalculateProgramacionFields(row, { persist: false }),
      ),
    );
    const vencidas = programaciones.filter(
      (row: any) => String(row.estado_programacion || '').toUpperCase() === 'VENCIDA',
    );

    const breakdown = eventos.reduce<Record<string, number>>((acc, event) => {
      const key = String(event.tipo_proceso || 'SIN_TIPO');
      acc[key] = (acc[key] ?? 0) + 1;
      return acc;
    }, {});
    const lubricantesRegistrados = new Set(
      allAnalysesForLubricants
        .map(
          (item) =>
            this.resolveLubricantIdentity(item).lubricante_lookup_key || null,
        )
        .filter(Boolean),
    ).size;

    return this.wrap(
      {
        generated_at: new Date().toISOString(),
        kpis: {
          procedimientos,
          analisis_lubricante: analisisTotal,
          lubricantes_registrados: lubricantesRegistrados,
          cronogramas_semanales: cronogramasTotal,
          reportes_diarios: reportesTotal,
          eventos_proceso: eventosTotal,
          programaciones_vencidas: vencidas.length,
          work_orders_pendientes: pendingWorkOrders,
          componentes_monitoreados: componentesTotal,
        },
        process_breakdown: Object.entries(breakdown).map(([tipo_proceso, total]) => ({
          tipo_proceso,
          total,
        })),
        recent_events: eventos.slice(0, 8),
        recent_analyses: analisisRows.slice(0, 5),
        recent_weekly_schedules: cronogramas.slice(0, 5),
        recent_daily_reports: reportes.slice(0, 5),
        component_highlights: componentesRecientes.slice(0, 5),
      },
      'Resumen de inteligencia operativa generado',
    );
  }

  async listAlertas(q: AlertaQueryDto) {
    const where: FindOptionsWhere<AlertaMantenimientoEntity> = {
      is_deleted: false,
    };
    if (q.estado) where.estado = this.normalizeAlertState(q.estado);
    if (q.nivel) where.nivel = this.normalizeAlertLevel(q.nivel);
    if (q.categoria) where.categoria = String(q.categoria).trim().toUpperCase();
    if (q.origen) where.origen = String(q.origen).trim().toUpperCase();
    if (q.tipo_alerta) where.tipo_alerta = q.tipo_alerta;
    if (q.equipo_id) where.equipo_id = q.equipo_id;
    const rows = await this.alertaRepo.find({
      where,
      order: { fecha_generada: 'DESC', id: 'DESC' },
    });
    const enriched = await this.enrichAlertRows(rows);
    const filtered = q.work_order_id
      ? enriched.filter((row: any) =>
          Array.isArray(row.work_orders)
            ? row.work_orders.some(
                (item: any) => String(item?.id || '') === String(q.work_order_id),
              )
            : String(row.work_order_id || '') === String(q.work_order_id),
        )
      : enriched;
    return this.wrap(filtered, 'Alertas listadas');
  }

  async getAlertasSummary() {
    const rows = await this.alertaRepo.find({
      where: { is_deleted: false },
      order: { fecha_generada: 'DESC', id: 'DESC' },
    });

    const totals = {
      total: rows.length,
      abiertas: 0,
      en_proceso: 0,
      resueltas: 0,
      cerradas: 0,
      critical: 0,
      warning: 0,
      info: 0,
    };
    const byCategory: Record<string, number> = {};
    const byOrigin: Record<string, number> = {};

    for (const row of rows) {
      const estado = this.normalizeAlertState(row.estado);
      const nivel = this.normalizeAlertLevel(row.nivel);
      const categoria = String(row.categoria || 'SIN_CATEGORIA')
        .trim()
        .toUpperCase();
      const origen = String(row.origen || 'SYSTEM').trim().toUpperCase();

      if (estado === 'ABIERTA') totals.abiertas += 1;
      else if (estado === 'EN_PROCESO') totals.en_proceso += 1;
      else if (estado === 'RESUELTA') totals.resueltas += 1;
      else if (estado === 'CERRADA') totals.cerradas += 1;

      if (nivel === 'CRITICAL') totals.critical += 1;
      else if (nivel === 'WARNING') totals.warning += 1;
      else totals.info += 1;

      byCategory[categoria] = (byCategory[categoria] ?? 0) + 1;
      byOrigin[origen] = (byOrigin[origen] ?? 0) + 1;
    }

    return this.wrap(
      {
        generated_at: new Date().toISOString(),
        totals,
        by_category: Object.entries(byCategory)
          .map(([categoria, total]) => ({ categoria, total }))
          .sort((a, b) => b.total - a.total),
        by_origin: Object.entries(byOrigin)
          .map(([origen, total]) => ({ origen, total }))
          .sort((a, b) => b.total - a.total),
      },
      'Resumen de alertas generado',
    );
  }

  async recalculateAlertas(source = 'manual') {
    const candidates = await this.buildAlertCandidates();
    const stats = await this.syncAlertCandidates(candidates);
    return this.wrap(
      {
        source,
        ...stats,
      },
      'Alertas recalculadas',
    );
  }

  private async processProgramacionesInWorkers(
    programaciones: ProgramacionPlanEntity[],
  ) {
    let cursor = 0;
    let upserts = 0;
    let skipped = 0;
    const errors: string[] = [];
    const workers = Math.max(
      1,
      Math.min(this.RECALCULATION_WORKERS, programaciones.length),
    );

    const runWorker = async () => {
      while (cursor < programaciones.length) {
        const index = cursor;
        cursor++;
        const outcome = await this.processProgramacion(programaciones[index]);
        if (outcome.upserted) upserts++;
        if (outcome.skipped) skipped++;
        if (outcome.error) errors.push(outcome.error);
      }
    };

    await Promise.all(Array.from({ length: workers }, () => runWorker()));
    return { upserts, skipped, errors };
  }

  private async processProgramacion(prog: ProgramacionPlanEntity) {
    try {
      if (!prog.plan_id || !prog.equipo_id) {
        return {
          upserted: false,
          skipped: true,
          error: `Programación inválida sin plan/equipo: ${prog.id ?? 'sin-id'}`,
        };
      }

      const equipo = await this.equipoRepo.findOne({
        where: { id: prog.equipo_id, is_deleted: false },
      });
      if (!equipo) {
        return {
          upserted: false,
          skipped: true,
          error: `Equipo no encontrado para programación ${prog.id ?? prog.plan_id}`,
        };
      }

      const h = Number(equipo.horometro_actual ?? 0);
      if (!Number.isFinite(h)) {
        return {
          upserted: false,
          skipped: true,
          error: `Horómetro inválido en equipo ${equipo.id}`,
        };
      }

      const pHRaw = prog.proxima_horas;
      const pH = pHRaw !== null && pHRaw !== undefined ? Number(pHRaw) : null;
      if (pHRaw !== null && pHRaw !== undefined && !Number.isFinite(pH)) {
        return {
          upserted: false,
          skipped: true,
          error: `proxima_horas inválida en programación ${prog.id ?? prog.plan_id}`,
        };
      }

      const pF = prog.proxima_fecha ? new Date(prog.proxima_fecha) : null;
      if (pF && Number.isNaN(pF.getTime())) {
        return {
          upserted: false,
          skipped: true,
          error: `proxima_fecha inválida en programación ${prog.id ?? prog.plan_id}`,
        };
      }

      const today = new Date();
      let tipo: string | null = null;
      let detalle = `Recalculada plan ${prog.plan_id}`;

      if (pH !== null && h >= pH) tipo = 'OVERDUE';
      else if (pF && today > pF) tipo = 'OVERDUE';
      else if (pH !== null) {
        const diff = pH - h;
        if (diff <= 325) tipo = 'MPG_325';
        else if (diff <= 650) tipo = 'MPG_650';
        else if (diff <= 975) tipo = 'MPG_975';
        else tipo = 'MPG_1300';
      } else if (pF) {
        tipo = 'MPG_1300';
      }

      if (!tipo) {
        return {
          upserted: false,
          skipped: true,
          error: `Sin criterio de alerta para programación ${prog.id ?? prog.plan_id}`,
        };
      }

      if (pH !== null && h < pH && tipo === 'MPG_1300') {
        detalle = `Programación activa fuera de umbrales para plan ${prog.plan_id}`;
      }

      const reference = `PLAN:${prog.plan_id}`;
      const existing = await this.alertaRepo.findOne({
        where: {
          equipo_id: prog.equipo_id,
          tipo_alerta: tipo,
          referencia: reference,
          estado: 'ABIERTA',
          is_deleted: false,
        },
      });
      if (!existing) {
        await this.alertaRepo.save(
          this.alertaRepo.create({
            equipo_id: prog.equipo_id,
            tipo_alerta: tipo,
            referencia: reference,
            detalle,
          }),
        );
        return { upserted: true, skipped: false, error: null as string | null };
      }

      return { upserted: false, skipped: false, error: null as string | null };
    } catch (e: any) {
      return {
        upserted: false,
        skipped: true,
        error: `Error procesando programación ${prog.id ?? prog.plan_id}: ${e?.message ?? 'desconocido'}`,
      };
    }
  }

  async listWorkOrders(q: WorkOrderQueryDto) {
    const qb = this.woRepo
      .createQueryBuilder('wo')
      .where('wo.is_deleted = false');
    if (q.equipo_id)
      qb.andWhere('wo.equipment_id = :equipo', { equipo: q.equipo_id });
    if (q.status_workflow)
      qb.andWhere('UPPER(wo.status_workflow) = :estado', {
        estado: this.normalizeWorkflowStatus(q.status_workflow),
      });
    if (q.maintenance_kind)
      qb.andWhere('wo.maintenance_kind = :kind', { kind: q.maintenance_kind });
    qb.orderBy('wo.created_at', 'DESC');
    const rows = await qb.getMany();
    return this.wrap(await Promise.all(rows.map((row) => this.enrichWorkOrder(row))), 'Work orders listadas');
  }

  async getWorkOrder(id: string) {
    const row = await this.findOneOrFail(this.woRepo, { id, is_deleted: false });
    return this.wrap(await this.enrichWorkOrder(row), 'Work order obtenida');
  }

  async createWorkOrder(dto: CreateWorkOrderDto) {
    if (dto.equipment_id) await this.findEquipoOrFail(dto.equipment_id);
    let resolvedPlanId = dto.plan_id ?? null;
    if (dto.procedimiento_id) {
      const synced = await this.syncPlanFromProcedimiento(dto.procedimiento_id);
      resolvedPlanId = synced.plan.id;
    }
    if (resolvedPlanId)
      await this.findOneOrFail(this.planRepo, {
        id: resolvedPlanId,
        is_deleted: false,
      });

    const normalizedStatus = this.normalizeWorkflowStatus(dto.status_workflow ?? 'PLANNED');
    let resolution = await this.resolveRequestedWorkOrderCode(dto.code);
    let created: WorkOrderEntity | null = null;

    for (let attempt = 0; attempt < 3; attempt += 1) {
      const entity = this.woRepo.create({
        code: resolution.resolvedCode,
        type: dto.type,
        equipment_id: dto.equipment_id ?? null,
        plan_id: resolvedPlanId,
        valor_json: {
          ...(dto.valor_json ?? {}),
          ...(dto.procedimiento_id
            ? { procedimiento_id: dto.procedimiento_id }
            : {}),
        },
        title: dto.title,
        description: dto.description ?? null,
        status_workflow: normalizedStatus,
        priority: dto.priority ?? 5,
        provider_type: dto.provider_type ?? 'INTERNO',
        maintenance_kind: dto.maintenance_kind ?? 'CORRECTIVO',
        safety_permit_required: dto.safety_permit_required ?? false,
        safety_permit_code: dto.safety_permit_code ?? null,
        vendor_id: dto.vendor_id ?? null,
        purchase_request_id: dto.purchase_request_id ?? null,
      });
      this.applyWorkflowDates(entity, null, normalizedStatus);

      try {
        created = await this.woRepo.save(entity);
        break;
      } catch (error: any) {
        if (!this.isDuplicateWorkOrderCodeError(error) || attempt >= 2) {
          throw error;
        }
        const nextCode = await this.generateNextWorkOrderCode();
        resolution = {
          requestedCode: resolution.requestedCode ?? dto.code ?? null,
          resolvedCode: nextCode,
          codeWasReassigned: true,
          reassignmentReason:
            resolution.reassignmentReason ||
            'El código solicitado ya no estaba disponible al momento de guardar.',
        };
      }
    }

    if (!created) {
      throw new ConflictException('No se pudo generar un código único para la orden de trabajo.');
    }

    await this.appendWorkOrderHistory(created.id, normalizedStatus, 'Orden de trabajo creada');
    if (dto.alerta_id) {
      await this.syncAlertWorkOrderLink(
        dto.alerta_id,
        created,
        normalizedStatus === 'CLOSED' ? 'CERRADA' : 'EN_PROCESO',
      );
    }
    const enriched = await this.enrichWorkOrder(created);
    const responsePayload = {
      ...enriched,
      requested_code: resolution.requestedCode,
      code_was_reassigned: resolution.codeWasReassigned,
      code_reassignment_reason: resolution.reassignmentReason,
    };
    await this.publishInAppNotification({
      title: 'Nueva orden de trabajo creada',
      body: resolution.codeWasReassigned
        ? `${enriched.code} - ${enriched.title}. Código ajustado automáticamente.`
        : `${enriched.code} - ${enriched.title}`,
      module: 'maintenance',
      entityType: 'work-order',
      entityId: created.id,
      level: normalizedStatus === 'CLOSED' ? 'success' : 'info',
    });
    await this.writeSecurityLog({
      description: `[WO:${created.id}] Creación de OT ${created.code}${resolution.codeWasReassigned ? ` (reemplazó ${resolution.requestedCode ?? 'sin código'})` : ''}`,
      typeLog: 'WORK_ORDER',
    });
    await this.registerProcessEvent({
      tipo_proceso: 'WORK_ORDER',
      accion: 'CREATED',
      referencia_tabla: 'tb_work_order',
      referencia_id: created.id,
      referencia_codigo: created.code,
      equipo_id: created.equipment_id ?? null,
      title: 'Orden de trabajo creada',
      body: `${enriched.code} - ${enriched.title}`,
      payload_kpi: {
        status_workflow: created.status_workflow,
        maintenance_kind: created.maintenance_kind,
      },
    });
    return this.wrap(responsePayload, 'Work order creada');
  }

  async updateWorkOrder(id: string, dto: UpdateWorkOrderDto) {
    const wo = await this.findOneOrFail(this.woRepo, { id, is_deleted: false });
    const previousStatus = this.normalizeWorkflowStatus(wo.status_workflow);
    let resolvedPlanId = wo.plan_id ?? null;
    if (dto.procedimiento_id) {
      const synced = await this.syncPlanFromProcedimiento(dto.procedimiento_id);
      resolvedPlanId = synced.plan.id;
    }
    Object.assign(wo, {
      ...dto,
      plan_id: resolvedPlanId,
      valor_json:
        dto.valor_json || dto.procedimiento_id
          ? {
              ...((wo.valor_json as Record<string, unknown> | null) ?? {}),
              ...(dto.valor_json ?? {}),
              ...(dto.procedimiento_id
                ? { procedimiento_id: dto.procedimiento_id }
                : {}),
            }
          : wo.valor_json,
    });
    wo.status_workflow = this.normalizeWorkflowStatus(dto.status_workflow ?? wo.status_workflow);
    this.applyWorkflowDates(wo, previousStatus, wo.status_workflow);
    const saved = await this.woRepo.save(wo);
    if (previousStatus !== saved.status_workflow) {
      await this.appendWorkOrderHistory(saved.id, saved.status_workflow, `Cambio de estado ${previousStatus} → ${saved.status_workflow}`, { fromStatus: previousStatus });
    } else {
      await this.appendWorkOrderHistory(saved.id, saved.status_workflow, 'Cabecera de OT actualizada', { fromStatus: previousStatus });
    }
    await this.syncAlertsForWorkOrder(saved);
    const enriched = await this.enrichWorkOrder(saved);
    await this.publishInAppNotification({
      title: previousStatus !== saved.status_workflow ? 'Estado de orden de trabajo actualizado' : 'Orden de trabajo actualizada',
      body:
        previousStatus !== saved.status_workflow
          ? `${enriched.code} cambió de ${previousStatus} a ${saved.status_workflow}`
          : `${enriched.code} - ${enriched.title} (${saved.status_workflow})`,
      module: 'maintenance',
      entityType: 'work-order',
      entityId: saved.id,
      level: saved.status_workflow === 'CLOSED' ? 'success' : 'info',
    });
    await this.writeSecurityLog({
      description: `[WO:${saved.id}] Actualización de OT ${saved.code} (${previousStatus} -> ${saved.status_workflow})`,
      typeLog: 'WORK_ORDER',
    });
    await this.registerProcessEvent({
      tipo_proceso: 'WORK_ORDER',
      accion: previousStatus !== saved.status_workflow ? 'STATUS_CHANGED' : 'UPDATED',
      referencia_tabla: 'tb_work_order',
      referencia_id: saved.id,
      referencia_codigo: saved.code,
      equipo_id: saved.equipment_id ?? null,
      title:
        previousStatus !== saved.status_workflow
          ? 'Estado de OT actualizado'
          : 'Orden de trabajo actualizada',
      body:
        previousStatus !== saved.status_workflow
          ? `${saved.code}: ${previousStatus} -> ${saved.status_workflow}`
          : `${saved.code} - ${enriched.title}`,
      payload_kpi: {
        status_workflow: saved.status_workflow,
        maintenance_kind: saved.maintenance_kind,
      },
    });
    return this.wrap(enriched, 'Work order actualizada');
  }

  async deleteWorkOrder(id: string) {
    const wo = await this.findOneOrFail(this.woRepo, { id, is_deleted: false });
    wo.is_deleted = true;
    await this.woRepo.save(wo);
    await this.appendWorkOrderHistory(wo.id, this.normalizeWorkflowStatus(wo.status_workflow), 'Orden de trabajo eliminada lógicamente', { fromStatus: wo.status_workflow });
    await this.writeSecurityLog({
      description: `[WO:${wo.id}] Eliminación lógica de OT ${wo.code}`,
      typeLog: 'WORK_ORDER',
    });
    return this.wrap(true, 'Work order eliminada');
  }

  async listWorkOrderTareas(workOrderId: string) {
    await this.findOneOrFail(this.woRepo, {
      id: workOrderId,
      is_deleted: false,
    });
    return this.wrap(
      await this.enrichWorkOrderTareas(
        await this.woTareaRepo.find({
          where: { work_order_id: workOrderId, is_deleted: false },
        }),
      ),
      'Tareas de OT listadas',
    );
  }

  async createWorkOrderTarea(
    workOrderId: string,
    dto: CreateWorkOrderTareaDto,
  ) {
    const workOrder = await this.findOneOrFail(this.woRepo, {
      id: workOrderId,
      is_deleted: false,
    });
    await this.findOneOrFail(this.planRepo, {
      id: dto.plan_id,
      is_deleted: false,
    });
    const taskDefinition = await this.findOneOrFail(this.planTareaRepo, {
      id: dto.tarea_id,
      plan_id: dto.plan_id,
      is_deleted: false,
    });
    if (workOrder.plan_id && workOrder.plan_id !== dto.plan_id) {
      throw new BadRequestException(
        'La tarea seleccionada no pertenece al plan operativo de la OT.',
      );
    }

    const normalized = this.normalizeWorkOrderTaskPayload(taskDefinition, dto);
    const existing = await this.woTareaRepo.findOne({
      where: {
        work_order_id: workOrderId,
        plan_id: dto.plan_id,
        tarea_id: dto.tarea_id,
        is_deleted: false,
      },
    });
    const created = await this.woTareaRepo.save(
      this.woTareaRepo.create({
        ...(existing ?? {}),
        work_order_id: workOrderId,
        plan_id: dto.plan_id,
        tarea_id: dto.tarea_id,
        valor_boolean: normalized.valor_boolean,
        valor_numeric: normalized.valor_numeric,
        valor_text: normalized.valor_text,
        valor_json: normalized.valor_json,
        observacion: normalized.observacion,
      }),
    );
    await this.appendWorkOrderHistory(
      workOrderId,
      this.normalizeWorkflowStatus(workOrder.status_workflow),
      existing
        ? `Tarea sincronizada: ${taskDefinition.actividad}`
        : `Tarea registrada: ${taskDefinition.actividad}`,
      { fromStatus: workOrder.status_workflow },
    );
    return this.wrap(
      (await this.enrichWorkOrderTareas([created]))[0] ?? created,
      existing ? 'Tarea de OT sincronizada' : 'Tarea de OT creada',
    );
  }

  async updateWorkOrderTarea(id: string, dto: UpdateWorkOrderTareaDto) {
    const tarea = await this.findOneOrFail(this.woTareaRepo, {
      id,
      is_deleted: false,
    });
    const definition = await this.findOneOrFail(this.planTareaRepo, {
      id: tarea.tarea_id,
      plan_id: tarea.plan_id,
      is_deleted: false,
    });
    const normalized = this.normalizeWorkOrderTaskPayload(definition, dto);
    Object.assign(tarea, {
      valor_boolean: normalized.valor_boolean,
      valor_numeric: normalized.valor_numeric,
      valor_text: normalized.valor_text,
      valor_json: normalized.valor_json,
      observacion: normalized.observacion,
      status: dto.status ?? tarea.status,
    });
    const saved = await this.woTareaRepo.save(tarea);
    const workOrder = await this.findOneOrFail(this.woRepo, {
      id: tarea.work_order_id,
      is_deleted: false,
    });
    await this.appendWorkOrderHistory(
      tarea.work_order_id,
      this.normalizeWorkflowStatus(workOrder.status_workflow),
      `Tarea actualizada: ${definition.actividad}`,
      { fromStatus: workOrder.status_workflow },
    );
    return this.wrap(
      (await this.enrichWorkOrderTareas([saved]))[0] ?? saved,
      'Tarea de OT actualizada',
    );
  }

  async deleteWorkOrderTarea(id: string) {
    const tarea = await this.findOneOrFail(this.woTareaRepo, {
      id,
      is_deleted: false,
    });
    tarea.is_deleted = true;
    await this.woTareaRepo.save(tarea);
    const workOrder = await this.findOneOrFail(this.woRepo, { id: tarea.work_order_id, is_deleted: false });
    await this.appendWorkOrderHistory(tarea.work_order_id, this.normalizeWorkflowStatus(workOrder.status_workflow), `Tarea eliminada: ${tarea.tarea_id}`, { fromStatus: workOrder.status_workflow });
    return this.wrap(true, 'Tarea de OT eliminada');
  }

  async uploadWorkOrderAdjunto(
    workOrderId: string,
    dto: UploadWorkOrderAdjuntoDto,
  ) {
    const workOrder = await this.findOneOrFail(this.woRepo, {
      id: workOrderId,
      is_deleted: false,
    });
    let buffer: Buffer;
    try {
      buffer = Buffer.from(dto.contenido_base64, 'base64');
    } catch {
      throw new BadRequestException('contenido_base64 inválido');
    }
    if (!buffer.length) throw new BadRequestException('Archivo vacío');
    const folder = join(this.uploadRoot, workOrderId);
    await mkdir(folder, { recursive: true });
    const originalName = basename(dto.nombre);
    const storageName = `${Date.now()}-${originalName.replace(/\s+/g, '_')}`;
    const filePath = join(folder, storageName);
    await writeFile(filePath, buffer);
    const hash = createHash('sha256').update(buffer).digest('hex');
    const created = await this.woAdjuntoRepo.save(
      this.woAdjuntoRepo.create({
        work_order_id: workOrderId,
        tipo: dto.tipo ?? 'EVIDENCIA',
        nombre: originalName,
        url: filePath,
        hash_sha256: hash,
        meta: {
          ...(dto.meta ?? {}),
          mime_type: dto.mime_type ?? null,
          extension: extname(originalName) || null,
          size_bytes: buffer.length,
        },
      }),
    );
    await this.appendWorkOrderHistory(workOrderId, this.normalizeWorkflowStatus(workOrder.status_workflow), `Adjunto agregado: ${originalName}`, { fromStatus: workOrder.status_workflow });
    await this.writeSecurityLog({
      description: `[WO:${workOrderId}] Adjunto agregado ${originalName}`,
      typeLog: 'ADJUNTO',
    });
    return this.wrap(
      {
        ...created,
        view_url: this.buildMaintenancePublicUrl(`/work-orders/${workOrderId}/adjuntos/${created.id}/view`),
      },
      'Adjunto cargado',
    );
  }

  async listWorkOrderAdjuntos(
    workOrderId: string,
    query: WorkOrderAdjuntoQueryDto,
  ) {
    await this.findOneOrFail(this.woRepo, {
      id: workOrderId,
      is_deleted: false,
    });
    const where: FindOptionsWhere<WorkOrderAdjuntoEntity> = {
      work_order_id: workOrderId,
      is_deleted: false,
    };
    if (query.tipo) where.tipo = query.tipo;
    const rows = await this.woAdjuntoRepo.find({ where });
    return this.wrap(
      rows.map((row) => ({
        ...row,
        view_url: this.buildMaintenancePublicUrl(`/work-orders/${workOrderId}/adjuntos/${row.id}/view`),
      })),
      'Adjuntos listados',
    );
  }

  async resolveWorkOrderAdjuntoFile(workOrderId: string, adjuntoId: string) {
    const adjunto = await this.findOneOrFail(this.woAdjuntoRepo, {
      id: adjuntoId,
      work_order_id: workOrderId,
      is_deleted: false,
    });
    const meta = (adjunto.meta ?? {}) as Record<string, unknown>;
    const mimeType =
      typeof meta.mime_type === 'string'
        ? meta.mime_type
        : 'application/octet-stream';
    return {
      filePath: adjunto.url,
      fileName: adjunto.nombre || `adjunto-${adjunto.id}`,
      mimeType,
      meta,
    };
  }

  async getWorkOrderAdjunto(workOrderId: string, adjuntoId: string) {
    const adjunto = await this.findOneOrFail(this.woAdjuntoRepo, {
      id: adjuntoId,
      work_order_id: workOrderId,
      is_deleted: false,
    });
    const file = await this.resolveWorkOrderAdjuntoFile(workOrderId, adjuntoId);
    return this.wrap(
      {
        id: adjunto.id,
        work_order_id: adjunto.work_order_id,
        tipo: adjunto.tipo,
        nombre: adjunto.nombre,
        hash_sha256: adjunto.hash_sha256,
        content_type: file.mimeType,
        view_url: this.buildMaintenancePublicUrl(`/work-orders/${workOrderId}/adjuntos/${adjuntoId}/view`),
        meta: adjunto.meta,
      },
      'Adjunto obtenido',
    );
  }

  async deleteWorkOrderAdjunto(workOrderId: string, adjuntoId: string) {
    const workOrder = await this.findOneOrFail(this.woRepo, { id: workOrderId, is_deleted: false });
    const adjunto = await this.findOneOrFail(this.woAdjuntoRepo, {
      id: adjuntoId,
      work_order_id: workOrderId,
      is_deleted: false,
    });
    adjunto.is_deleted = true;
    await this.woAdjuntoRepo.save(adjunto);
    try {
      await unlink(adjunto.url);
    } catch {
      /* ignore */
    }
    await this.appendWorkOrderHistory(workOrderId, this.normalizeWorkflowStatus(workOrder.status_workflow), `Adjunto eliminado: ${adjunto.nombre ?? adjunto.id}`, { fromStatus: workOrder.status_workflow });
    await this.writeSecurityLog({
      description: `[WO:${workOrderId}] Adjunto eliminado ${adjunto.id}`,
      typeLog: 'ADJUNTO',
    });
    return this.wrap(true, 'Adjunto eliminado');
  }

  async getInventoryCostReference(productoId: string, bodegaId: string) {
    return this.wrap(
      await this.resolveInventoryCostReference(productoId, bodegaId),
      'Costo de referencia obtenido',
    );
  }

  async listConsumos(workOrderId: string) {
    await this.findOneOrFail(this.woRepo, { id: workOrderId, is_deleted: false });
    const rows = await this.consumoRepo.find({
      where: { work_order_id: workOrderId, is_deleted: false },
      order: { id: 'DESC' },
    });
    const { productMap, warehouseMap } = await this.buildInventoryCatalogMaps(
      rows.map((row) => row.producto_id),
      rows.map((row) => row.bodega_id || '').filter(Boolean),
    );
    return this.wrap(
      rows.map((row) => this.mapConsumoWithCatalogs(row, productMap, warehouseMap)),
      'Consumos listados',
    );
  }

  async listWorkOrderHistory(workOrderId: string) {
    await this.findOneOrFail(this.woRepo, { id: workOrderId, is_deleted: false });
    const rows = await this.woHistoryRepo.find({
      where: { work_order_id: workOrderId },
      order: { changed_at: 'DESC' },
    });
    return this.wrap(rows, 'Historial de la OT listado');
  }

  async createConsumo(workOrderId: string, dto: CreateConsumoDto) {
    const workOrder = await this.findOneOrFail(this.woRepo, {
      id: workOrderId,
      is_deleted: false,
    });
    if (!dto.bodega_id) {
      throw new BadRequestException('La bodega es obligatoria para registrar el consumo.');
    }

    const { producto, bodega } = await this.validateProductoEnBodega(dto.producto_id, dto.bodega_id);
    const costReference = await this.resolveInventoryCostReference(dto.producto_id, dto.bodega_id);
    const costoUnitario = this.toNumeric(dto.costo_unitario, costReference.costo_unitario);
    const subtotal = dto.cantidad * costoUnitario;
    const saved = await this.consumoRepo.save(
      this.consumoRepo.create({
        ...dto,
        costo_unitario: costoUnitario,
        work_order_id: workOrderId,
        subtotal,
      }),
    );
    await this.appendWorkOrderHistory(workOrderId, this.normalizeWorkflowStatus(workOrder.status_workflow), `Consumo registrado para producto ${dto.producto_id} por ${dto.cantidad}`, { fromStatus: workOrder.status_workflow });
    await this.writeSecurityLog({
      description: `[WO:${workOrderId}] Consumo registrado producto ${dto.producto_id} cantidad ${dto.cantidad}`,
      typeLog: 'CONSUMO',
    });
    return this.wrap(
      this.mapConsumoWithCatalogs(saved, new Map([[producto.id, producto]]), new Map([[bodega.id, bodega]])),
      'Consumo registrado',
    );
  }

  async listIssueMaterials(workOrderId: string) {
    await this.findOneOrFail(this.woRepo, { id: workOrderId, is_deleted: false });
    const entregas = await this.dataSource.getRepository(EntregaMaterialEntity).find({
      where: { work_order_id: workOrderId, is_deleted: false },
      order: { fecha: 'DESC' },
    });
    const entregaIds = entregas.map((item) => item.id);
    const detalles = entregaIds.length
      ? await this.dataSource.getRepository(EntregaMaterialDetEntity).find({ where: { entrega_id: In(entregaIds) } })
      : [];

    const { productMap, warehouseMap } = await this.buildInventoryCatalogMaps(
      detalles.map((item) => item.producto_id),
      detalles.map((item) => item.bodega_id),
    );

    return this.wrap(
      entregas.map((entrega) => {
        const items = detalles
          .filter((detalle) => detalle.entrega_id === entrega.id)
          .map((detalle) => this.mapIssueItemWithCatalogs(detalle, productMap, warehouseMap));
        return {
          ...entrega,
          items,
          total: items.reduce(
            (acc, item) => acc + Number(item.costo_unitario || 0) * Number(item.cantidad || 0),
            0,
          ),
        };
      }),
      'Salidas de materiales listadas',
    );
  }

  async issueMaterials(workOrderId: string, dto: IssueMaterialsDto) {
    const qr = this.dataSource.createQueryRunner();
    await qr.connect();
    await qr.startTransaction();
    try {
      const workOrder = await this.findOneOrFail(this.woRepo, {
        id: workOrderId,
        is_deleted: false,
      });
      const em = await qr.manager.save(
        EntregaMaterialEntity,
        qr.manager.create(EntregaMaterialEntity, {
          work_order_id: workOrderId,
          code: `EM-${Date.now()}`,
          observacion: dto.observacion,
        }),
      );
      const mov = await qr.manager.save(
        MovimientoInventarioEntity,
        qr.manager.create(MovimientoInventarioEntity, {
          tipo_movimiento: 'SALIDA',
          work_order_id: workOrderId,
          total_costos: 0,
        }),
      );
      let total = 0;
      for (const item of dto.items) {
        const reserva = await qr.manager.findOne(ReservaStockEntity, {
          where: {
            work_order_id: workOrderId,
            producto_id: item.producto_id,
            bodega_id: item.bodega_id,
            estado: 'RESERVADO',
            is_deleted: false,
          },
        });
        if (!reserva || Number(reserva.cantidad) < item.cantidad)
          throw new ConflictException('Reserva insuficiente');
        const stock = await qr.manager.findOne(StockBodegaEntity, {
          where: { producto_id: item.producto_id, bodega_id: item.bodega_id },
        });
        if (!stock || Number(stock.stock_actual) < item.cantidad)
          throw new ConflictException('Stock insuficiente');
        const producto = await qr.manager.findOne(ProductoEntity, {
          where: { id: item.producto_id },
        });
        if (!producto) throw new NotFoundException('Producto no encontrado');
        const costo = Number(producto.ultimo_costo);
        const subtotal = item.cantidad * costo;
        total += subtotal;
        stock.stock_actual = Number(stock.stock_actual) - item.cantidad;
        await qr.manager.save(stock);
        reserva.estado = 'CONSUMIDO';
        await qr.manager.save(reserva);
        await qr.manager.save(
          EntregaMaterialDetEntity,
          qr.manager.create(EntregaMaterialDetEntity, {
            entrega_id: em.id,
            producto_id: item.producto_id,
            bodega_id: item.bodega_id,
            cantidad: item.cantidad,
            costo_unitario: costo,
          }),
        );
        const movDet = await qr.manager.save(
          MovimientoInventarioDetEntity,
          qr.manager.create(MovimientoInventarioDetEntity, {
            movimiento_id: mov.id,
            producto_id: item.producto_id,
            cantidad: item.cantidad,
            costo_unitario: costo,
            subtotal_costo: subtotal,
          }),
        );
        await qr.manager.save(
          KardexEntity,
          qr.manager.create(KardexEntity, {
            bodega_id: item.bodega_id,
            producto_id: item.producto_id,
            movimiento_id: mov.id,
            movimiento_det_id: movDet.id,
            tipo_movimiento: 'SALIDA',
            salida_cantidad: item.cantidad,
            costo_unitario: costo,
            costo_total: subtotal,
            saldo_cantidad: stock.stock_actual,
            saldo_costo_promedio: costo,
            saldo_valorizado: Number(stock.stock_actual) * costo,
          }),
        );
        await qr.manager.save(
          ConsumoRepuestoEntity,
          qr.manager.create(ConsumoRepuestoEntity, {
            work_order_id: workOrderId,
            producto_id: item.producto_id,
            bodega_id: item.bodega_id,
            cantidad: item.cantidad,
            costo_unitario: costo,
            subtotal,
          }),
        );
      }
      mov.total_costos = total;
      await qr.manager.save(mov);
      await qr.commitTransaction();
      await this.appendWorkOrderHistory(workOrderId, this.normalizeWorkflowStatus(workOrder.status_workflow), `Salida de materiales registrada (${dto.items.length} items)`, { fromStatus: workOrder.status_workflow });
      await this.writeSecurityLog({
        description: `[WO:${workOrderId}] Emisión de materiales por total ${total}`,
        typeLog: 'MATERIALES',
      });
      return this.wrap(
        { entrega_id: em.id, movimiento_id: mov.id, total },
        'Materiales entregados',
      );
    } catch (e) {
      await qr.rollbackTransaction();
      throw e;
    } finally {
      await qr.release();
    }
  }

  private async findEquipoOrFail(id: string) {
    return this.findOneOrFail(this.equipoRepo, { id, is_deleted: false });
  }
  private async findOneOrFail<T extends ObjectLiteral>(
    repo: Repository<T>,
    where: FindOptionsWhere<T>,
  ) {
    const item = await repo.findOne({ where });
    if (!item) throw new NotFoundException('Registro no encontrado');
    return item;
  }

  async seedEstadosCatalogo() {
    const codigos = ['OPERATIVO', 'RESERVA', 'MPG', 'CORRECTIVO', 'BLOQUEADA'];
    for (const codigo of codigos) {
      const existing = await this.estadoCatalogoRepo.findOne({
        where: { codigo, is_deleted: false },
      });
      if (!existing)
        await this.estadoCatalogoRepo.save(
          this.estadoCatalogoRepo.create({ codigo, descripcion: codigo }),
        );
    }
  }
}
