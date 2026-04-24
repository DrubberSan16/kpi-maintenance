import {
  BadRequestException,
  ConflictException,
  ForbiddenException,
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
import nodemailer, { type Transporter } from 'nodemailer';
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
  InventorySucursalEntity,
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
  ProgramacionMensualDetalleEntity,
  ProgramacionMensualEntity,
  ProgramacionPlanEntity,
  ReservaStockEntity,
  ReporteCombustibleEntity,
  ReporteOperacionDiariaEntity,
  ReporteOperacionDiariaUnidadEntity,
  WorkOrderStatusHistoryEntity,
  StockBodegaEntity,
  UnidadMedidaEntity,
  WorkOrderAdjuntoEntity,
  WorkOrderEntity,
  WorkOrderTareaEntity,
} from '../entities/kpi-maintenance.entity';
import {
  AnalisisAceiteKpiQueryDto,
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
  CreateProgramacionMensualDetalleDto,
  CreateReporteOperacionDiariaDto,
  CreateWorkOrderDto,
  CreateWorkOrderTareaDto,
  DateRangeDto,
  EquipoCriticidadEnum,
  EquipoQueryDto,
  EquipoEstadoOperativoEnum,
  EquipoTipoQueryDto,
  EventoProcesoQueryDto,
  IntelligencePeriodQueryDto,
  ImportAnalisisLubricanteBatchDto,
  IssueMaterialsDto,
  PurgeAnalisisLubricanteDto,
  ProgramacionMensualQueryDto,
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
  UpdateProgramacionMensualConfigDto,
  UpdateProgramacionMensualDetalleDto,
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

type InventoryAlertItem = {
  stock_id: string;
  producto_id: string;
  producto_codigo: string | null;
  producto_nombre: string | null;
  producto_label: string;
  bodega_id: string;
  bodega_codigo: string | null;
  bodega_nombre: string | null;
  bodega_label: string;
  stock_actual: number;
  stock_min_bodega: number;
  stock_max_bodega: number;
  costo_promedio_bodega: number;
  nivel: AlertLevel;
  observacion: string;
  actor_username: string | null;
};

type SucursalScopeContext = {
  sucursalId: string;
  locationIds: Set<string>;
  locationTokens: Set<string>;
  warehouseIds: Set<string>;
  equipmentIds: Set<string>;
  equipmentCodes: Set<string>;
};

type RequestActorContext = {
  userId?: string | null;
  username?: string | null;
  displayName?: string | null;
};

type SecurityUserDirectoryItem = {
  id: string | null;
  nameUser: string | null;
  nameSurname: string | null;
  email: string | null;
  roleName: string | null;
  roleNames: string[];
  status: string | null;
  isDeleted: boolean;
};

type AlertNotificationRecipient = {
  type: 'TRANSACTION_OWNER' | 'GENERAL_MANAGER' | 'ADMINISTRATOR';
  email: string;
  userId?: string | null;
  username?: string | null;
  displayName?: string | null;
  roleName?: string | null;
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

type ParsedProgramacionMensualWorkbook = {
  header: {
    codigo: string;
    fecha_inicio: string | null;
    fecha_fin: string | null;
    sucursal_id?: string | null;
    locacion?: string | null;
    documento_origen: string;
    nombre_archivo: string;
    resumen: string;
    payload_json: Record<string, unknown>;
  };
  detalles: Array<{
    equipo_id?: string | null;
    equipo_codigo: string;
    equipo_nombre?: string | null;
    fecha_programada: string;
    dia_mes: number;
    valor_crudo: string;
    valor_normalizado: string;
    tipo_mantenimiento: string;
    frecuencia_horas?: number | null;
    procedimiento_id?: string | null;
    plan_id?: string | null;
    es_sincronizable: boolean;
    observacion?: string | null;
    orden: number;
    payload_json: Record<string, unknown>;
  }>;
  warnings: string[];
};

type ParsedCronogramaSemanalWorkbook = {
  dto: CreateCronogramaSemanalDto;
  warnings: string[];
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

const PROGRAMACION_MPG_FREQUENCIES = new Set([
  250,
  325,
  500,
  650,
  975,
  1000,
  1300,
]);

const DEFAULT_PROGRAMACION_MONTHLY_COLOR_PALETTE = {
  MPG: '#F4DD6B',
  HORAS_PROGRAMADAS: '#F4DD6B',
  MANTENIMIENTO: '#F4DD6B',
  OTRO: '#D7E0EA',
  SEMANAL: '#9EC5FE',
  SINCRONIZADO: '#8ED1A5',
  DEFAULT: '#D7E0EA',
} as const;

const EQUIPO_CRITICIDAD_VALUES = Object.values(EquipoCriticidadEnum);
const EQUIPO_ESTADO_OPERATIVO_VALUES = Object.values(EquipoEstadoOperativoEnum);

const LUBRICANT_IMPORT_PARAMETER_ROWS = [
  { row: 22, label: 'Viscosidad a 100ºC, cSt' },
  { row: 23, label: 'Viscosidad a 40ºC, cSt' },
  { row: 24, label: 'Indice de Viscosidad' },
  { row: 25, label: 'T.B.N. mgKOH/gr' },
  { row: 26, label: 'Humedad' },
  { row: 27, label: 'Glycol, Abs/cm' },
  { row: 28, label: 'Combustible' },
  { row: 32, label: 'Oxidación, Abs/cm' },
  { row: 33, label: 'Nitración, Abs/cm' },
  { row: 34, label: 'Sulfatación, Abs/cm' },
  { row: 35, label: 'Hollín, wt%' },
  { row: 39, label: 'Si (Silicio)' },
  { row: 40, label: 'Na (Sodio)' },
  { row: 41, label: 'Vanadio (V)' },
  { row: 42, label: 'Ni (Niquel)' },
  { row: 46, label: 'Fe (Hierro)' },
  { row: 47, label: 'Cr (Cromo)' },
  { row: 48, label: 'Al (Aluminio)' },
  { row: 49, label: 'Cu (Cobre)' },
  { row: 50, label: 'Pb (Plomo)' },
  { row: 51, label: 'Estaño (Sn)' },
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
  private inventoryImportSuppressed = false;

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
    @InjectRepository(InventorySucursalEntity)
    private readonly sucursalRepo: Repository<InventorySucursalEntity>,
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
    @InjectRepository(ProgramacionMensualEntity)
    private readonly programacionMensualRepo: Repository<ProgramacionMensualEntity>,
    @InjectRepository(ProgramacionMensualDetalleEntity)
    private readonly programacionMensualDetRepo: Repository<ProgramacionMensualDetalleEntity>,
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
    @InjectRepository(UnidadMedidaEntity)
    private readonly unidadMedidaRepo: Repository<UnidadMedidaEntity>,
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
  private readonly lubricantImportTemplatePaths = [
    process.env.LUBRICANT_IMPORT_TEMPLATE_PATH,
    join(
      process.cwd(),
      'templates',
      'analisis-lubricante',
      'FORMATO_CARGA_ANALISIS_LUBRICANTE.xlsx',
    ),
    join(
      process.cwd(),
      'kpi-maintenance',
      'templates',
      'analisis-lubricante',
      'FORMATO_CARGA_ANALISIS_LUBRICANTE.xlsx',
    ),
  ].filter((value): value is string => Boolean(value));
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

  private buildSucursalLabel(sucursal?: Partial<InventorySucursalEntity> | null) {
    if (!sucursal) return null;
    const codigo = String(sucursal.codigo || '').trim();
    const nombre = String(sucursal.nombre || '').trim();
    if (codigo && nombre) return `${codigo} - ${nombre}`;
    return nombre || codigo || null;
  }

  private async resolveSucursalForWrite(
    explicitSucursalId?: string | null,
    scopedSucursalId?: string | null,
  ) {
    const normalizedSucursalId = this.firstNonEmptyString(
      explicitSucursalId,
      scopedSucursalId,
    );
    if (!normalizedSucursalId) return null;
    const sucursal = await this.sucursalRepo.findOne({
      where: {
        id: normalizedSucursalId,
        is_deleted: false,
      },
    });
    if (!sucursal) {
      throw new BadRequestException('La sucursal seleccionada no existe.');
    }
    return sucursal;
  }

  private normalizeScopeToken(value: unknown) {
    return String(value ?? '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .trim()
      .toUpperCase();
  }

  private async buildSucursalScopeContext(
    sucursalId?: string | null,
  ): Promise<SucursalScopeContext | null> {
    const normalizedSucursalId = String(sucursalId || '').trim();
    if (!normalizedSucursalId) return null;

    const sucursal = await this.sucursalRepo.findOne({
      where: { id: normalizedSucursalId, is_deleted: false },
    });
    if (!sucursal) {
      throw new BadRequestException('La sucursal seleccionada no existe.');
    }

    const [locations, warehouses] = await Promise.all([
      this.locationRepo.find({
        where: { sucursal_id: normalizedSucursalId, is_deleted: false } as any,
      }),
      this.bodegaRepo.find({
        where: { sucursal_id: normalizedSucursalId, is_deleted: false } as any,
      }),
    ]);

    const locationIds = new Set(
      locations.map((item) => String(item.id || '').trim()).filter(Boolean),
    );
    const warehouseIds = new Set(
      warehouses.map((item) => String(item.id || '').trim()).filter(Boolean),
    );
    const locationTokens = new Set<string>();

    locations.forEach((item) => {
      [item.id, item.codigo, item.nombre].forEach((value) => {
        const token = this.normalizeScopeToken(value);
        if (token) locationTokens.add(token);
      });
    });

    const equipments = locationIds.size
      ? await this.equipoRepo.find({
          where: {
            location_id: In([...locationIds]),
            is_deleted: false,
          } as any,
        })
      : [];
    const equipmentIds = new Set(
      equipments.map((item) => String(item.id || '').trim()).filter(Boolean),
    );
    const equipmentCodes = new Set(
      equipments
        .map((item) => this.normalizeScopeToken(item.codigo))
        .filter(Boolean),
    );

    return {
      sucursalId: normalizedSucursalId,
      locationIds,
      locationTokens,
      warehouseIds,
      equipmentIds,
      equipmentCodes,
    };
  }

  private matchesScopedLocation(
    value: unknown,
    scope: SucursalScopeContext | null,
  ) {
    if (!scope) return true;
    const token = this.normalizeScopeToken(value);
    return token ? scope.locationTokens.has(token) : false;
  }

  private matchesScopedEquipment(
    equipmentId: unknown,
    equipmentCode: unknown,
    scope: SucursalScopeContext | null,
  ) {
    if (!scope) return true;
    const normalizedEquipmentId = String(equipmentId || '').trim();
    if (normalizedEquipmentId && scope.equipmentIds.has(normalizedEquipmentId)) {
      return true;
    }
    const normalizedEquipmentCode = this.normalizeScopeToken(equipmentCode);
    return normalizedEquipmentCode
      ? scope.equipmentCodes.has(normalizedEquipmentCode)
      : false;
  }

  private async assertWarehouseVisibleForSucursal(
    bodegaId?: string | null,
    sucursalId?: string | null,
  ) {
    const normalizedBodegaId = String(bodegaId || '').trim();
    const normalizedSucursalId = String(sucursalId || '').trim();
    if (!normalizedBodegaId || !normalizedSucursalId) return;

    const bodega = await this.bodegaRepo.findOne({
      where: {
        id: normalizedBodegaId,
        is_deleted: false,
      } as any,
    });

    if (!bodega || String(bodega.sucursal_id || '').trim() !== normalizedSucursalId) {
      throw new NotFoundException(
        'La bodega seleccionada no pertenece a la sucursal activa.',
      );
    }
  }

  private isProcedimientoVisibleForScope(
    row: Pick<ProcedimientoPlantillaEntity, 'bodega_id'>,
    scope: SucursalScopeContext | null,
  ) {
    if (!scope) return true;
    const normalizedWarehouseId = String(row.bodega_id || '').trim();
    if (!normalizedWarehouseId) return true;
    return scope.warehouseIds.has(normalizedWarehouseId);
  }

  private buildProgramacionMensualPeriods(details: Array<{ fecha_programada?: string | null; programacion_id?: string | null }>) {
    const periodsMap = new Map<
      string,
      { period: string; total: number; sincronizados: number; label: string }
    >();

    for (const detail of details) {
      const period = String(detail.fecha_programada || '').slice(0, 7);
      if (!period) continue;
      const parsed = new Date(`${period}-01T00:00:00Z`);
      const label = parsed.toLocaleDateString('es-EC', {
        month: 'long',
        year: 'numeric',
        timeZone: 'UTC',
      });
      const current = periodsMap.get(period) ?? {
        period,
        total: 0,
        sincronizados: 0,
        label,
      };
      current.total += 1;
      if (detail.programacion_id) current.sincronizados += 1;
      periodsMap.set(period, current);
    }

    return [...periodsMap.values()].sort((a, b) =>
      String(a.period).localeCompare(String(b.period)),
    );
  }

  private scopeProgramacionMensualPayload(
    payload: any,
    scope: SucursalScopeContext | null,
  ) {
    if (!scope) return payload;
    if (String(payload?.sucursal_id || '').trim() === scope.sucursalId) {
      return payload;
    }
    if (this.matchesScopedLocation(payload?.locacion, scope)) {
      return payload;
    }

    const detalles = Array.isArray(payload?.detalles)
      ? payload.detalles.filter((item: any) =>
          this.matchesScopedEquipment(item?.equipo_id, item?.equipo_codigo, scope),
        )
      : [];
    const detallesConsolidados = Array.isArray(payload?.detalles_consolidados)
      ? payload.detalles_consolidados.filter((item: any) =>
          this.matchesScopedEquipment(item?.equipo_id, item?.equipo_codigo, scope),
        )
      : [];

    if (!detalles.length && !detallesConsolidados.length) {
      return null;
    }

    const detailSource = detallesConsolidados.length ? detallesConsolidados : detalles;

    return {
      ...payload,
      total_detalles: detalles.length,
      total_detalles_consolidados: detallesConsolidados.length,
      periodos: this.buildProgramacionMensualPeriods(detailSource),
      detalles,
      detalles_consolidados: detallesConsolidados,
    };
  }

  private scopeCronogramaSemanalPayload(
    payload: any,
    scope: SucursalScopeContext | null,
  ) {
    if (!scope) return payload;
    if (String(payload?.sucursal_id || '').trim() === scope.sucursalId) {
      return payload;
    }
    if (this.matchesScopedLocation(payload?.locacion, scope)) {
      return payload;
    }

    const detalles = Array.isArray(payload?.detalles)
      ? payload.detalles.filter((item: any) =>
          this.matchesScopedEquipment(item?.equipo_id, item?.equipo_codigo, scope),
        )
      : [];

    if (!detalles.length) {
      return null;
    }

    const dailyHours = detalles.reduce((acc: Record<string, number>, item: any) => {
      const key = String(item?.fecha_actividad || '').slice(0, 10);
      if (!key) return acc;
      acc[key] = Number(
        ((acc[key] ?? 0) + this.toNumeric(item?.duracion_horas, 0)).toFixed(2),
      );
      return acc;
    }, {});

    const dailyEquipmentMap = new Map<string, any>();
    for (const item of detalles) {
      const fechaActividad = String(item?.fecha_actividad || '').slice(0, 10);
      const equipoCodigo = String(item?.equipo_codigo || '').trim();
      if (!fechaActividad || !equipoCodigo) continue;
      const key = `${fechaActividad}::${this.normalizeWorkbookToken(equipoCodigo)}`;
      const current = dailyEquipmentMap.get(key) ?? {
        key,
        fecha_actividad: fechaActividad,
        equipo_id: item?.equipo_id ?? null,
        equipo_codigo: item?.equipo_codigo ?? null,
        equipo_nombre: item?.equipo_nombre ?? null,
        total_horas: 0,
        total_actividades: 0,
        cronograma_ids: Array.isArray(payload?.id) ? payload.id : [payload?.id].filter(Boolean),
        cronograma_codigos: [payload?.codigo].filter(Boolean),
        actividades: [],
      };
      current.total_horas = Number(
        (current.total_horas + this.toNumeric(item?.duracion_horas, 0)).toFixed(2),
      );
      current.total_actividades += 1;
      current.actividades.push({
        detalle_id: item?.id,
        actividad: item?.actividad,
        tipo_proceso: item?.tipo_proceso ?? null,
        hora_inicio: item?.hora_inicio ?? null,
        hora_fin: item?.hora_fin ?? null,
        duracion_horas: this.toNumeric(item?.duracion_horas, 0),
        responsable_area: item?.responsable_area ?? null,
        observacion: item?.observacion ?? null,
      });
      dailyEquipmentMap.set(key, current);
    }

    const timeSlots = [...new Set(detalles.map((item: any) => `${item?.hora_inicio || ''}-${item?.hora_fin || ''}`))]
      .filter(Boolean)
      .map((key) => {
        const [hora_inicio = null, hora_fin = null] = String(key).split('-');
        return {
          key,
          hora_inicio,
          hora_fin,
          label:
            hora_inicio && hora_fin
              ? `${String(hora_inicio).slice(0, 5)} - ${String(hora_fin).slice(0, 5)}`
              : String(key),
        };
      });

    return {
      ...payload,
      detalles,
      daily_hours: dailyHours,
      daily_equipment_hours: [...dailyEquipmentMap.values()].sort((a, b) =>
        `${a.fecha_actividad}-${a.equipo_codigo || ''}`.localeCompare(
          `${b.fecha_actividad}-${b.equipo_codigo || ''}`,
        ),
      ),
      time_slots: timeSlots,
    };
  }

  private scopeReporteDiarioPayload(
    payload: any,
    scope: SucursalScopeContext | null,
  ) {
    if (!scope) return payload;
    if (String(payload?.sucursal_id || '').trim() === scope.sucursalId) {
      return payload;
    }
    if (this.matchesScopedLocation(payload?.locacion, scope)) {
      return payload;
    }

    const unidades = Array.isArray(payload?.unidades)
      ? payload.unidades.filter((item: any) =>
          this.matchesScopedEquipment(item?.equipo_id, item?.equipo_codigo, scope),
        )
      : [];
    const componentes = Array.isArray(payload?.componentes)
      ? payload.componentes.filter((item: any) =>
          this.matchesScopedEquipment(item?.equipo_id, item?.equipo_codigo, scope),
        )
      : [];

    if (!unidades.length && !componentes.length) {
      return null;
    }

    return {
      ...payload,
      unidades,
      combustibles: [],
      componentes,
    };
  }

  private async filterWorkOrdersByScope(
    rows: WorkOrderEntity[],
    scope: SucursalScopeContext | null,
  ) {
    if (!scope || !rows.length) return rows;

    const visibleIds = new Set<string>();
    const pendingIds: string[] = [];

    for (const row of rows) {
      const workOrderId = String(row.id || '').trim();
      if (!workOrderId) continue;
      if (
        row.equipment_id &&
        scope.equipmentIds.has(String(row.equipment_id || '').trim())
      ) {
        visibleIds.add(workOrderId);
        continue;
      }
      pendingIds.push(workOrderId);
    }

    if (pendingIds.length && scope.warehouseIds.size) {
      const warehouseIds = [...scope.warehouseIds];
      const [consumos, reservas, entregas] = await Promise.all([
        this.consumoRepo.find({
          where: {
            work_order_id: In(pendingIds),
            bodega_id: In(warehouseIds),
            is_deleted: false,
          } as any,
        }),
        this.reservaRepo.find({
          where: {
            work_order_id: In(pendingIds),
            bodega_id: In(warehouseIds),
            is_deleted: false,
          } as any,
        }),
        this.dataSource.getRepository(EntregaMaterialEntity).find({
          where: {
            work_order_id: In(pendingIds),
            is_deleted: false,
          } as any,
        }),
      ]);

      consumos.forEach((row) => visibleIds.add(String(row.work_order_id || '').trim()));
      reservas.forEach((row) => visibleIds.add(String(row.work_order_id || '').trim()));

      const entregaById = new Map(
        entregas.map((row) => [String(row.id || '').trim(), String(row.work_order_id || '').trim()]),
      );
      const entregaIds = [...entregaById.keys()].filter(Boolean);
      if (entregaIds.length) {
        const detalles = await this.dataSource
          .getRepository(EntregaMaterialDetEntity)
          .find({
            where: {
              entrega_id: In(entregaIds),
              bodega_id: In(warehouseIds),
            } as any,
          });
        detalles.forEach((detalle) => {
          const workOrderId = entregaById.get(String(detalle.entrega_id || '').trim());
          if (workOrderId) visibleIds.add(workOrderId);
        });
      }
    }

    return rows.filter((row) => visibleIds.has(String(row.id || '').trim()));
  }

  private async assertWorkOrderVisibleForSucursal(
    row: WorkOrderEntity,
    sucursalId?: string | null,
  ) {
    const scope = await this.buildSucursalScopeContext(sucursalId);
    const visible = await this.filterWorkOrdersByScope([row], scope);
    if (!visible.length) {
      throw new NotFoundException('Work order no encontrada');
    }
    return visible[0];
  }

  private async ensureInventoryWarehouseExists(bodegaId?: string | null) {
    const normalized = String(bodegaId ?? '').trim();
    if (!normalized) return null;
    const bodega = await this.bodegaRepo.findOne({
      where: { id: normalized, is_deleted: false },
    });
    if (!bodega) {
      throw new NotFoundException('La bodega seleccionada no existe.');
    }
    return bodega;
  }

  private async buildInventoryCatalogMaps(productIds: string[], warehouseIds: string[]) {
    const uniqueProductIds = [...new Set(productIds.filter(Boolean))];
    const uniqueWarehouseIds = [...new Set(warehouseIds.filter(Boolean))];

    const [productos, bodegas] = await Promise.all([
      uniqueProductIds.length
        ? this.productoRepo.find({
            where: { id: In(uniqueProductIds), is_deleted: false },
          })
        : Promise.resolve([] as ProductoEntity[]),
      uniqueWarehouseIds.length
        ? this.bodegaRepo.find({
            where: { id: In(uniqueWarehouseIds), is_deleted: false },
          })
        : Promise.resolve([] as BodegaEntity[]),
    ]);

    return {
      productMap: new Map(productos.map((item) => [item.id, item])),
      warehouseMap: new Map(bodegas.map((item) => [item.id, item])),
    };
  }

  private parseOilUsageDate(
    value: unknown,
    options?: { endOfDay?: boolean },
  ) {
    const raw = this.safeDateOnlyString(value) ?? String(value || '').trim();
    if (!raw) return null;
    const parsed = new Date(
      /^\d{4}-\d{2}-\d{2}$/.test(raw)
        ? `${raw}T${options?.endOfDay ? '23:59:59.999' : '00:00:00.000'}`
        : raw,
    );
    if (Number.isNaN(parsed.getTime())) return null;
    if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
      parsed.setHours(
        options?.endOfDay ? 23 : 0,
        options?.endOfDay ? 59 : 0,
        options?.endOfDay ? 59 : 0,
        options?.endOfDay ? 999 : 0,
      );
    }
    return parsed;
  }

  private formatOilUsageDateLabel(value: Date) {
    return new Intl.DateTimeFormat('es-EC', {
      day: '2-digit',
      month: 'short',
      year: 'numeric',
    }).format(value);
  }

  private formatOilUsageBucketLabel(value: Date, periodo: string) {
    if (periodo === 'ANUAL') {
      return new Intl.DateTimeFormat('es-EC', {
        month: 'short',
      }).format(value);
    }
    return new Intl.DateTimeFormat('es-EC', {
      day: '2-digit',
      month: 'short',
    }).format(value);
  }

  private buildOilUsageDateRange(query?: AnalisisAceiteKpiQueryDto) {
    const normalizedPeriod = String(query?.periodo || 'MENSUAL')
      .trim()
      .toUpperCase();
    const today = new Date();
    const referenceDate =
      this.parseOilUsageDate(query?.reference_date) ??
      this.parseOilUsageDate(query?.from) ??
      this.parseOilUsageDate(query?.to, { endOfDay: true }) ??
      today;

    let fromDate: Date;
    let toDate: Date;

    if (normalizedPeriod === 'SEMANAL') {
      fromDate = new Date(referenceDate);
      fromDate.setHours(0, 0, 0, 0);
      fromDate.setDate(fromDate.getDate() - fromDate.getDay());
      toDate = new Date(fromDate);
      toDate.setDate(toDate.getDate() + 6);
      toDate.setHours(23, 59, 59, 999);
    } else if (normalizedPeriod === 'ANUAL') {
      const year = Number(query?.year) || referenceDate.getFullYear();
      fromDate = new Date(year, 0, 1, 0, 0, 0, 0);
      toDate = new Date(year, 11, 31, 23, 59, 59, 999);
    } else if (normalizedPeriod === 'PERSONALIZADO') {
      fromDate =
        this.parseOilUsageDate(query?.from) ??
        new Date(referenceDate.getFullYear(), referenceDate.getMonth(), referenceDate.getDate(), 0, 0, 0, 0);
      toDate =
        this.parseOilUsageDate(query?.to, { endOfDay: true }) ??
        new Date(referenceDate.getFullYear(), referenceDate.getMonth(), referenceDate.getDate(), 23, 59, 59, 999);
    } else {
      const year = Number(query?.year) || referenceDate.getFullYear();
      const month = Number(query?.month) || referenceDate.getMonth() + 1;
      fromDate = new Date(year, month - 1, 1, 0, 0, 0, 0);
      toDate = new Date(year, month, 0, 23, 59, 59, 999);
    }

    if (fromDate.getTime() > toDate.getTime()) {
      const swappedFrom = new Date(toDate);
      swappedFrom.setHours(0, 0, 0, 0);
      const swappedTo = new Date(fromDate);
      swappedTo.setHours(23, 59, 59, 999);
      fromDate = swappedFrom;
      toDate = swappedTo;
    }

    let label = `${this.formatOilUsageDateLabel(fromDate)} - ${this.formatOilUsageDateLabel(toDate)}`;
    if (normalizedPeriod === 'MENSUAL') {
      label = new Intl.DateTimeFormat('es-EC', {
        month: 'long',
        year: 'numeric',
      }).format(fromDate);
    } else if (normalizedPeriod === 'ANUAL') {
      label = `Año ${fromDate.getFullYear()}`;
    } else if (normalizedPeriod === 'SEMANAL') {
      label = `Semana del ${this.formatOilUsageDateLabel(fromDate)} al ${this.formatOilUsageDateLabel(toDate)}`;
    }

    return {
      periodo:
        normalizedPeriod === 'SEMANAL' ||
        normalizedPeriod === 'ANUAL' ||
        normalizedPeriod === 'PERSONALIZADO'
          ? normalizedPeriod
          : 'MENSUAL',
      fromDate,
      toDate,
      from: fromDate.toISOString().slice(0, 10),
      to: toDate.toISOString().slice(0, 10),
      year: fromDate.getFullYear(),
      month: fromDate.getMonth() + 1,
      label,
      reference_date: referenceDate.toISOString().slice(0, 10),
    };
  }

  private resolveWorkOrderReferenceDate(workOrder?: WorkOrderEntity | null) {
    if (!workOrder) return null;
    return (
      this.parseOilUsageDate(workOrder.closed_at, { endOfDay: true }) ??
      this.parseOilUsageDate(workOrder.started_at, { endOfDay: true }) ??
      this.parseOilUsageDate(workOrder.scheduled_start, { endOfDay: true }) ??
      this.parseOilUsageDate(workOrder.updated_at, { endOfDay: true }) ??
      this.parseOilUsageDate(workOrder.created_at, { endOfDay: true })
    );
  }

  private async buildOilProductCatalog() {
    const unitNames = ['GALON', 'GALONES'];
    const unitCodes = ['GALON', 'GALONES', 'GAL', 'GL'];
    const { entities, raw } = await this.productoRepo
      .createQueryBuilder('producto')
      .leftJoin(
        UnidadMedidaEntity,
        'unidad',
        'unidad.id = producto.unidad_medida_id AND unidad.is_deleted = false',
      )
      .where('producto.is_deleted = false')
      .andWhere(
        `(
          UPPER(COALESCE(unidad.nombre, '')) IN (:...unitNames)
          OR UPPER(COALESCE(unidad.abreviatura, '')) IN (:...unitCodes)
          OR UPPER(COALESCE(unidad.codigo, '')) IN (:...unitCodes)
        )`,
        {
          unitNames,
          unitCodes,
        },
      )
      .select('producto')
      .addSelect('unidad.nombre', 'unidad_nombre')
      .addSelect('unidad.abreviatura', 'unidad_abreviatura')
      .addSelect('unidad.codigo', 'unidad_codigo')
      .orderBy('producto.nombre', 'ASC')
      .addOrderBy('producto.codigo', 'ASC')
      .getRawAndEntities();

    return entities.map((producto, index) => {
      const unidadNombre = String(raw[index]?.unidad_nombre || '').trim() || null;
      const unidadAbreviatura =
        String(raw[index]?.unidad_abreviatura || '').trim() || null;
      const unidadCodigo = String(raw[index]?.unidad_codigo || '').trim() || null;
      return {
        id: producto.id,
        codigo: producto.codigo ?? null,
        nombre: producto.nombre ?? null,
        label: this.buildProductoLabel(producto) ?? producto.id,
        unidad_medida: unidadNombre,
        unidad_medida_abreviatura: unidadAbreviatura,
        unidad_medida_codigo: unidadCodigo,
      };
    });
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
      cantidad_reservada: null,
      cantidad_emitida: null,
      cantidad_pendiente: null,
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

  private async calculatePlannedAndIssuedMaterialTotals(
    workOrderId: string,
    productoId: string,
    bodegaId: string,
  ) {
    const consumos = await this.consumoRepo.find({
      where: {
        work_order_id: workOrderId,
        producto_id: productoId,
        bodega_id: bodegaId,
        is_deleted: false,
      },
    });
    const plannedQty = consumos.reduce(
      (acc, row) => acc + this.toNumeric(row.cantidad, 0),
      0,
    );

    const entregas = await this.dataSource.getRepository(EntregaMaterialEntity).find({
      where: { work_order_id: workOrderId, is_deleted: false },
    });
    const entregaIds = entregas.map((item) => item.id);
    const detalles = entregaIds.length
      ? await this.dataSource.getRepository(EntregaMaterialDetEntity).find({
          where: {
            entrega_id: In(entregaIds),
            producto_id: productoId,
            bodega_id: bodegaId,
          },
        })
      : [];
    const issuedQty = detalles.reduce(
      (acc, row) => acc + this.toNumeric(row.cantidad, 0),
      0,
    );

    return {
      plannedQty,
      issuedQty,
      pendingQty: Math.max(plannedQty - issuedQty, 0),
    };
  }

  private async upsertReservedMaterial(
    workOrderId: string,
    productoId: string,
    bodegaId: string,
    quantityDelta: number,
  ) {
    const normalizedDelta = this.toNumeric(quantityDelta, 0);
    if (normalizedDelta <= 0) return null;

    const existing = await this.reservaRepo.findOne({
      where: {
        work_order_id: workOrderId,
        producto_id: productoId,
        bodega_id: bodegaId,
        estado: 'RESERVADO',
        is_deleted: false,
      },
    });

    if (existing) {
      existing.cantidad = this.toNumeric(existing.cantidad, 0) + normalizedDelta;
      return this.reservaRepo.save(existing);
    }

    return this.reservaRepo.save(
      this.reservaRepo.create({
        work_order_id: workOrderId,
        producto_id: productoId,
        bodega_id: bodegaId,
        cantidad: normalizedDelta,
        estado: 'RESERVADO',
      }),
    );
  }

  private async rebuildPendingReservaFromConsumos(
    workOrderId: string,
    productoId: string,
    bodegaId: string,
    manager?: any,
  ) {
    const totals = await this.calculatePlannedAndIssuedMaterialTotals(
      workOrderId,
      productoId,
      bodegaId,
    );

    if (totals.pendingQty <= 0) {
      return null;
    }

    const where = {
      work_order_id: workOrderId,
      producto_id: productoId,
      bodega_id: bodegaId,
      estado: 'RESERVADO',
      is_deleted: false,
    };

    const existing = manager
      ? await manager.findOne(ReservaStockEntity, { where })
      : await this.reservaRepo.findOne({ where });

    if (existing) {
      existing.cantidad = totals.pendingQty;
      existing.estado = 'RESERVADO';
      return manager
        ? manager.save(ReservaStockEntity, existing)
        : this.reservaRepo.save(existing);
    }

    const created = manager
      ? manager.create(ReservaStockEntity, {
          work_order_id: workOrderId,
          producto_id: productoId,
          bodega_id: bodegaId,
          cantidad: totals.pendingQty,
          estado: 'RESERVADO',
        })
      : this.reservaRepo.create({
          work_order_id: workOrderId,
          producto_id: productoId,
          bodega_id: bodegaId,
          cantidad: totals.pendingQty,
          estado: 'RESERVADO',
        });

    return manager
      ? manager.save(ReservaStockEntity, created)
      : this.reservaRepo.save(created);
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
    void this.recalculateAlertas(source)
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
    const normalizedSource = String(source || 'manual').trim().toLowerCase();
    if (normalizedSource === 'inventory-kardex-import-started') {
      this.inventoryImportSuppressed = true;
      return this.wrap(
        { accepted: true, source, inventory_import_running: true },
        'Importacion de inventario marcada en proceso',
      );
    }
    if (
      normalizedSource === 'inventory-kardex-import-completed' ||
      normalizedSource === 'inventory-kardex-import-failed'
    ) {
      this.inventoryImportSuppressed = false;
    }

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

  private normalizeEquipoCriticidad(value: unknown): string | undefined {
    const raw = String(value ?? '').trim().toUpperCase();
    if (!raw) return undefined;
    if (['BAJA', 'LOW', 'BAJO'].includes(raw)) {
      return EquipoCriticidadEnum.BAJA;
    }
    if (['MEDIA', 'MEDIO', 'MEDIUM'].includes(raw)) {
      return EquipoCriticidadEnum.MEDIA;
    }
    if (['ALTA', 'ALTO', 'HIGH'].includes(raw)) {
      return EquipoCriticidadEnum.ALTA;
    }
    if (['CRITICA', 'CRÍTICA', 'CRITICO', 'CRÍTICO', 'CRITICAL'].includes(raw)) {
      return EquipoCriticidadEnum.CRITICA;
    }
    throw new BadRequestException(
      `La criticidad del equipo no es válida. Valores permitidos: ${EQUIPO_CRITICIDAD_VALUES.join(', ')}.`,
    );
  }

  private normalizeEquipoEstadoOperativo(value: unknown): string | undefined {
    const raw = String(value ?? '').trim().toUpperCase();
    if (!raw) return undefined;
    if (['OPERATIVO', 'OPERANDO', 'DISPONIBLE'].includes(raw)) {
      return EquipoEstadoOperativoEnum.OPERATIVO;
    }
    if (['RESERVA', 'STANDBY'].includes(raw)) {
      return EquipoEstadoOperativoEnum.RESERVA;
    }
    if (['MPG', 'PREVENTIVO'].includes(raw)) {
      return EquipoEstadoOperativoEnum.MPG;
    }
    if (['CORRECTIVO', 'REPARACION', 'REPARACIÓN'].includes(raw)) {
      return EquipoEstadoOperativoEnum.CORRECTIVO;
    }
    if (['BLOQUEADA', 'BLOQUEADO', 'NO_OPERATIVO', 'NO OPERATIVO'].includes(raw)) {
      return EquipoEstadoOperativoEnum.BLOQUEADA;
    }
    throw new BadRequestException(
      `El estado operativo del equipo no es válido. Valores permitidos: ${EQUIPO_ESTADO_OPERATIVO_VALUES.join(', ')}.`,
    );
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

  private resolveAlertReferenceDisplay(
    row: Pick<AlertaMantenimientoEntity, 'referencia' | 'referencia_tipo'>,
    payload: Record<string, unknown>,
  ) {
    const fallback =
      String(row.referencia || '').trim() ||
      String(row.referencia_tipo || '').trim() ||
      'Sin referencia';

    if (row.referencia_tipo === 'PROGRAMACION') {
      const label = this.firstNonEmptyString(
        payload.referencia_label,
        payload.procedimiento_nombre,
        payload.plan_nombre,
        payload.plan_codigo,
        payload.programacion_codigo,
      );
      return label ? `Programación · ${label}` : 'Programación de mantenimiento';
    }

    if (row.referencia_tipo === 'REPORTE_DIARIO') {
      const label = this.firstNonEmptyString(
        payload.reporte_codigo,
        payload.fecha_reporte,
      );
      return label ? `Reporte diario · ${label}` : fallback;
    }

    if (row.referencia_tipo === 'ANALISIS_LUBRICANTE') {
      const label = this.firstNonEmptyString(payload.codigo);
      return label ? `Análisis · ${label}` : fallback;
    }

    if (row.referencia_tipo === 'COMBUSTIBLE') {
      const tanque = this.firstNonEmptyString(payload.tanque);
      return tanque ? `Tanque ${tanque}` : fallback;
    }

    if (row.referencia_tipo === 'STOCK_BODEGA') {
      const label = [
        String(payload.producto_label || '').trim(),
        String(payload.bodega_label || '').trim(),
      ]
        .filter(Boolean)
        .join(' · ');
      return label || fallback;
    }

    return fallback;
  }

  private buildCompletedAlertDetail(
    alerta: Pick<AlertaMantenimientoEntity, 'origen' | 'detalle'>,
    payload: Record<string, unknown>,
    workOrder: Partial<WorkOrderEntity>,
  ) {
    const workOrderLabel =
      [String(workOrder.code || '').trim(), String(workOrder.title || '').trim()]
        .filter(Boolean)
        .join(' · ') ||
      String(workOrder.id || '').trim() ||
      'OT';

    if (alerta.origen === 'PROGRAMACION') {
      const planLabel =
        this.firstNonEmptyString(
          payload.procedimiento_nombre,
          payload.plan_nombre,
          payload.plan_codigo,
          payload.programacion_codigo,
        ) || 'Mantenimiento';
      return `${planLabel} · se culminó la OT ${workOrderLabel}`;
    }

    const currentDetail = String(alerta.detalle || '').trim();
    return currentDetail
      ? `${currentDetail} · se culminó la OT ${workOrderLabel}`
      : `Se culminó la OT ${workOrderLabel}`;
  }

  private async syncProgramacionExecutionFromAlert(
    alerta: Pick<AlertaMantenimientoEntity, 'origen' | 'payload_json'>,
    workOrder: Partial<WorkOrderEntity>,
  ) {
    if (alerta.origen !== 'PROGRAMACION') return;
    const payload = (alerta.payload_json ?? {}) as Record<string, unknown>;
    const programacionId = String(payload.programacion_id || '').trim();
    if (!programacionId) return;

    const programacion = await this.programacionRepo.findOne({
      where: { id: programacionId, is_deleted: false },
    });
    if (!programacion) return;

    const closedAtRaw =
      workOrder.closed_at ||
      (workOrder as Record<string, unknown>).updated_at ||
      new Date().toISOString();
    const closedAt = new Date(String(closedAtRaw));
    const executionDate = Number.isNaN(closedAt.getTime())
      ? new Date().toISOString().slice(0, 10)
      : closedAt.toISOString().slice(0, 10);

    programacion.ultima_ejecucion_fecha = executionDate;

    if (workOrder.equipment_id) {
      const equipo = await this.equipoRepo.findOne({
        where: { id: workOrder.equipment_id, is_deleted: false },
      });
      if (equipo?.horometro_actual != null) {
        programacion.ultima_ejecucion_horas = this.toNumeric(
          equipo.horometro_actual,
          0,
        );
      }
    }

    await this.recalculateProgramacionFields(programacion);
  }

  private async syncProgramacionExecutionFromLinkedWorkOrder(
    workOrder: Partial<WorkOrderEntity>,
  ) {
    const workOrderId = String(workOrder.id || '').trim();
    if (!workOrderId) return;
    const programacion = await this.programacionRepo.findOne({
      where: { work_order_id: workOrderId, is_deleted: false, activo: true },
    });
    if (!programacion) return;

    const closedAtRaw =
      workOrder.closed_at ||
      (workOrder as Record<string, unknown>).updated_at ||
      new Date().toISOString();
    const closedAt = new Date(String(closedAtRaw));
    const executionDate = Number.isNaN(closedAt.getTime())
      ? new Date().toISOString().slice(0, 10)
      : closedAt.toISOString().slice(0, 10);

    programacion.ultima_ejecucion_fecha = executionDate;

    if (workOrder.equipment_id) {
      const equipo = await this.equipoRepo.findOne({
        where: { id: workOrder.equipment_id, is_deleted: false },
      });
      if (equipo?.horometro_actual != null) {
        programacion.ultima_ejecucion_horas = this.toNumeric(
          equipo.horometro_actual,
          0,
        );
      }
    }

    await this.recalculateProgramacionFields(programacion);
  }

  private async applyClosedWorkOrderAlertOutcome(
    alerta: AlertaMantenimientoEntity,
    workOrder: Partial<WorkOrderEntity>,
    snapshots: Array<{
      id: string;
      code: string | null;
      title: string | null;
      status_workflow: string | null;
    }>,
  ) {
    const payload = {
      ...((alerta.payload_json ?? {}) as Record<string, unknown>),
    };
    payload.work_orders = snapshots;
    payload.completion_status = 'OT_CULMINADA';
    payload.completed_work_order = {
      id: String(workOrder.id || '').trim() || null,
      code: String(workOrder.code || '').trim() || null,
      title: String(workOrder.title || '').trim() || null,
      status_workflow: this.normalizeWorkflowStatus(workOrder.status_workflow),
    };
    payload.referencia_label =
      this.firstNonEmptyString(
        payload.referencia_label,
        payload.procedimiento_nombre,
        payload.plan_nombre,
        payload.plan_codigo,
        payload.programacion_codigo,
      ) || null;

    alerta.payload_json = payload;
    alerta.work_order_id =
      snapshots[snapshots.length - 1]?.id ||
      String(workOrder.id || '').trim() ||
      null;
    alerta.estado = 'CERRADA';
    alerta.nivel = 'INFO';
    alerta.detalle = this.buildCompletedAlertDetail(alerta, payload, workOrder);
    alerta.ultima_evaluacion_at = new Date();
    alerta.resolved_at = new Date();

    await this.syncProgramacionExecutionFromAlert(alerta, workOrder);
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
    const resolvedState =
      nextAlertState ??
      this.resolveAlertStateFromLinkedWorkOrders(snapshots, alerta.estado);
    alerta.estado = resolvedState;
    if (resolvedState === 'CERRADA') {
      await this.applyClosedWorkOrderAlertOutcome(alerta, workOrder, snapshots);
    }
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
      const resolvedState = this.resolveAlertStateFromLinkedWorkOrders(
        snapshots,
        alerta.estado,
      );
      alerta.estado = resolvedState;
      if (resolvedState === 'CERRADA') {
        await this.applyClosedWorkOrderAlertOutcome(alerta, workOrder, snapshots);
      }
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

  private readonly alertMailFromAddress = String(
    process.env.ALERT_EMAIL_FROM ||
      process.env.MAIL_FROM_ADDRESS ||
      process.env.SMTP_FROM_EMAIL ||
      '',
  ).trim();

  private readonly alertMailFromName = String(
    process.env.ALERT_EMAIL_FROM_NAME ||
      process.env.MAIL_FROM_NAME ||
      'Justice KPI Alerts',
  ).trim();

  private readonly alertGeneralManagerEmail = String(
    process.env.ALERT_GENERAL_MANAGER_EMAIL ||
      process.env.GENERAL_MANAGER_EMAIL ||
      '',
  ).trim();

  private readonly alertAdministratorEmail = String(
    process.env.ALERT_ADMINISTRATOR_EMAIL ||
      process.env.ADMINISTRATOR_EMAIL ||
      '',
  ).trim();

  private securityUsersCache:
    | { expiresAt: number; items: SecurityUserDirectoryItem[] }
    | null = null;
  private mailTransporter: Transporter | null = null;
  private mailTransportVerified = false;

  private toNumeric(value: unknown, fallback = 0) {
    const num = Number(value);
    return Number.isFinite(num) ? num : fallback;
  }

  private normalizeWorkflowStatus(value: unknown) {
    const raw = String(value || '').trim().toUpperCase();
    if (['PLANNED', 'PLANIFICADA', 'PLANIFICADO', 'CREADA', 'CREADO'].includes(raw)) return 'PLANNED';
    if (['IN_PROGRESS', 'IN PROGRESS', 'EN_PROCESO', 'EN PROCESO', 'PROCESSING'].includes(raw)) return 'IN_PROGRESS';
    if (['BLOCKED', 'BLOQUEADA', 'BLOQUEADO', 'ON_HOLD', 'DETENIDA', 'DETENIDO'].includes(raw)) return 'BLOCKED';
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

  private async generateNextProgramacionMensualCode() {
    const rows = await this.programacionMensualRepo.find({
      select: { codigo: true, id: true },
    });
    const codes = rows
      .map((row) => String(row.codigo || '').trim())
      .filter(Boolean)
      .sort(
        (a, b) =>
          this.getAlphaNumericCodeRank('PMM', b) -
          this.getAlphaNumericCodeRank('PMM', a),
      );
    return this.computeNextAlphaNumericCode('PMM', codes[0] ?? null);
  }

  private async generateNextCronogramaSemanalCode() {
    const rows = await this.cronogramaSemanalRepo.find({
      select: { codigo: true, id: true },
    });
    const codes = rows
      .map((row) => String(row.codigo || '').trim())
      .filter(Boolean)
      .sort(
        (a, b) =>
          this.getAlphaNumericCodeRank('PCS', b) -
          this.getAlphaNumericCodeRank('PCS', a),
      );
    return this.computeNextAlphaNumericCode('PCS', codes[0] ?? null);
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

  async getNextCronogramaSemanalCode() {
    return this.wrap(
      { code: await this.generateNextCronogramaSemanalCode() },
      'Siguiente código de cronograma semanal generado',
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

  private buildFrontendPublicUrl(path: string) {
    const normalizedPath = path.startsWith('/') ? path : `/${path}`;
    return this.publicBaseUrl ? `${this.publicBaseUrl}${normalizedPath}` : `/app${normalizedPath}`;
  }

  private getPublicSiteOrigin() {
    if (!this.publicBaseUrl) return '';
    try {
      const parsed = new URL(this.publicBaseUrl);
      return parsed.origin;
    } catch {
      return '';
    }
  }

  private buildMaintenanceFilePublicUrl(path: string) {
    const relative = this.buildMaintenanceRelativePath(path);
    const siteOrigin = this.getPublicSiteOrigin();
    return siteOrigin ? `${siteOrigin}${relative}` : relative;
  }

  private buildWorkOrderAdjuntoLinks(workOrderId: string, adjuntoId: string) {
    return {
      view_url: this.buildMaintenanceFilePublicUrl(
        `/public/work-orders/${workOrderId}/adjuntos/${adjuntoId}/view`,
      ),
      download_url: this.buildMaintenanceFilePublicUrl(
        `/public/work-orders/${workOrderId}/adjuntos/${adjuntoId}/download`,
      ),
      public_page_url: this.buildFrontendPublicUrl(
        `/adjuntos/ot/${workOrderId}/${adjuntoId}`,
      ),
    };
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

  private async getJson(url: string) {
    const response = await fetch(url, { method: 'GET' });
    if (!response.ok) {
      const body = await response.text().catch(() => '');
      throw new Error(`HTTP ${response.status}: ${body || response.statusText}`);
    }
    return response.json().catch(() => null);
  }

  private unwrapServiceData<T>(input: any): T {
    if (input && typeof input === 'object' && 'data' in input) {
      return input.data as T;
    }
    return input as T;
  }

  private firstNonEmptyString(...values: unknown[]) {
    for (const value of values) {
      const normalized = String(value ?? '').trim();
      if (normalized) return normalized;
    }
    return null;
  }

  private normalizeEmail(value: unknown) {
    const normalized = String(value ?? '').trim().toLowerCase();
    if (!normalized) return null;
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(normalized) ? normalized : null;
  }

  private normalizeUsername(value: unknown) {
    return String(value ?? '').trim().toLowerCase() || null;
  }

  private normalizeRoleName(value: unknown) {
    return String(value ?? '')
      .trim()
      .toUpperCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '');
  }

  private extractAlertActorHints(payload: Record<string, unknown>) {
    const sampleInfo =
      payload.sample_info && typeof payload.sample_info === 'object'
        ? (payload.sample_info as Record<string, unknown>)
        : {};
    return {
      userId: this.firstNonEmptyString(
        payload.actor_user_id,
        payload.user_id,
        payload.requested_user_id,
        sampleInfo.actor_user_id,
      ),
      username: this.firstNonEmptyString(
        payload.actor_username,
        payload.updated_by,
        payload.created_by,
        payload.requested_by,
        payload.registrado_por,
        payload.source_updated_by,
        payload.source_created_by,
        sampleInfo.actor_username,
      ),
      email: this.normalizeEmail(
        this.firstNonEmptyString(
          payload.actor_email,
          payload.updated_by_email,
          payload.created_by_email,
          payload.requested_by_email,
          payload.source_updated_by_email,
          payload.source_created_by_email,
          sampleInfo.actor_email,
        ),
      ),
      displayName: this.firstNonEmptyString(
        payload.actor_name,
        payload.requested_by_name,
        sampleInfo.actor_name,
      ),
      roleName: this.firstNonEmptyString(payload.actor_role, sampleInfo.actor_role),
    };
  }

  private async fetchSecurityUsers(force = false) {
    const now = Date.now();
    if (
      !force &&
      this.securityUsersCache &&
      this.securityUsersCache.expiresAt > now
    ) {
      return this.securityUsersCache.items;
    }
    if (!this.securityServiceUrl) return [];

    try {
      const separator = this.securityServiceUrl.includes('?') ? '&' : '?';
      const raw = await this.getJson(
        `${this.securityServiceUrl}/users${separator}includeDeleted=true`,
      );
      const items = this.unwrapServiceData<any[]>(raw);
      const normalized = Array.isArray(items)
        ? items.map((item) => {
            const rawRoleNames = [
              item?.role?.nombre,
              item?.roleName,
              item?.role_nombre,
              ...(Array.isArray(item?.roles)
                ? item.roles.flatMap((role: any) => [
                    role?.nombre,
                    role?.name,
                    role?.roleName,
                  ])
                : []),
            ]
              .map((value) => this.firstNonEmptyString(value))
              .filter((value): value is string => Boolean(value));

            return {
              id: this.firstNonEmptyString(item?.id),
              nameUser: this.firstNonEmptyString(item?.nameUser),
              nameSurname: this.firstNonEmptyString(item?.nameSurname),
              email: this.normalizeEmail(item?.email),
              roleName: rawRoleNames[0] ?? null,
              roleNames: [...new Set(rawRoleNames)],
              status: this.firstNonEmptyString(item?.status),
              isDeleted: Boolean(item?.isDeleted ?? item?.is_deleted ?? false),
            };
          })
        : [];
      this.securityUsersCache = {
        expiresAt: now + 5 * 60 * 1000,
        items: normalized,
      };
      return normalized;
    } catch (error: any) {
      this.logger.warn(
        `No se pudo consultar usuarios de seguridad: ${error?.message ?? 'desconocido'}`,
      );
      return [];
    }
  }

  private findSecurityUserByHints(
    users: SecurityUserDirectoryItem[],
    hints: {
      userId?: string | null;
      username?: string | null;
      email?: string | null;
    },
  ) {
    const userId = this.firstNonEmptyString(hints.userId);
    const username = this.normalizeUsername(hints.username);
    const email = this.normalizeEmail(hints.email);

    return (
      users.find(
        (item) =>
          !item.isDeleted &&
          String(item.status || 'ACTIVE').trim().toUpperCase() === 'ACTIVE' &&
          ((userId && item.id === userId) ||
            (username &&
              this.normalizeUsername(item.nameUser) === username) ||
            (email && this.normalizeEmail(item.email) === email)),
      ) ?? null
    );
  }

  private isActiveSecurityUser(item: SecurityUserDirectoryItem) {
    return (
      !item.isDeleted &&
      String(item.status || 'ACTIVE').trim().toUpperCase() === 'ACTIVE'
    );
  }

  private findSecurityUsersByRole(
    users: SecurityUserDirectoryItem[],
    matcher: (roleName: string) => boolean,
  ) {
    return users.filter((item) => {
      if (!this.isActiveSecurityUser(item)) return false;
      const roleNames = [
        item.roleName,
        ...(Array.isArray(item.roleNames) ? item.roleNames : []),
      ]
        .map((value) => this.normalizeRoleName(value))
        .filter(Boolean);
      return roleNames.some((roleName) => matcher(roleName));
    });
  }

  private getAlertNotificationRecipientsUserIds(
    recipients: AlertNotificationRecipient[],
  ) {
    return [
      ...new Set(
        recipients
          .map((item) => String(item.userId || '').trim())
          .filter(Boolean),
      ),
    ];
  }

  private getAlertNotificationRecipientTokens(
    recipients: AlertNotificationRecipient[],
  ) {
    return [
      ...new Set(
        recipients
          .flatMap((item) => [
            String(item.userId || '').trim(),
            this.normalizeEmail(item.email) ?? '',
            this.normalizeUsername(item.username) ?? '',
          ])
          .filter(Boolean),
      ),
    ];
  }

  private async resolveAlertNotificationRecipients(
    payload: Record<string, unknown>,
  ) {
    const users = await this.fetchSecurityUsers();
    const actorHints = this.extractAlertActorHints(payload);
    const actorUser = this.findSecurityUserByHints(users, actorHints);
    const managerUsers = this.findSecurityUsersByRole(users, (roleName) =>
      ['GERENTE GENERAL', 'GERENCIA GENERAL'].some((expected) =>
        roleName.includes(expected),
      ),
    );
    const adminUsers = this.findSecurityUsersByRole(users, (roleName) =>
      ['ADMINISTRADOR', 'ADMIN', 'SUPER ADMIN', 'SUPERADMIN'].some((expected) =>
        roleName.includes(expected),
      ),
    );

    const candidates: AlertNotificationRecipient[] = [];

    const pushRecipient = (
      type: AlertNotificationRecipient['type'],
      emailValue: unknown,
      options?: {
        userId?: string | null;
        username?: string | null;
        displayName?: string | null;
        roleName?: string | null;
      },
    ) => {
      const email = this.normalizeEmail(emailValue);
      if (!email) return;
      candidates.push({
        type,
        email,
        userId: options?.userId ?? null,
        username: options?.username ?? null,
        displayName: options?.displayName ?? null,
        roleName: options?.roleName ?? null,
      });
    };

    pushRecipient(
      'TRANSACTION_OWNER',
      actorUser?.email ?? actorHints.email,
      actorUser
        ? {
            userId: actorUser.id,
            username: actorUser.nameUser,
            displayName: actorUser.nameSurname ?? actorUser.nameUser,
            roleName: actorUser.roleName,
          }
        : {
            userId: actorHints.userId,
            username: actorHints.username,
            displayName: actorHints.displayName,
            roleName: actorHints.roleName,
          },
    );
    if (managerUsers.length) {
      for (const managerUser of managerUsers) {
        pushRecipient('GENERAL_MANAGER', managerUser.email, {
          userId: managerUser.id,
          username: managerUser.nameUser,
          displayName: managerUser.nameSurname ?? managerUser.nameUser,
          roleName: managerUser.roleName,
        });
      }
    } else {
      pushRecipient('GENERAL_MANAGER', this.alertGeneralManagerEmail, {
        displayName: 'Gerencia General',
        roleName: 'GERENTE GENERAL',
      });
    }

    if (adminUsers.length) {
      for (const adminUser of adminUsers) {
        pushRecipient('ADMINISTRATOR', adminUser.email, {
          userId: adminUser.id,
          username: adminUser.nameUser,
          displayName: adminUser.nameSurname ?? adminUser.nameUser,
          roleName: adminUser.roleName,
        });
      }
    } else {
      pushRecipient('ADMINISTRATOR', this.alertAdministratorEmail, {
        displayName: 'Administrador',
        roleName: 'ADMINISTRADOR',
      });
    }

    const deduped = new Map<string, AlertNotificationRecipient>();
    for (const item of candidates) {
      if (!deduped.has(item.email)) {
        deduped.set(item.email, item);
      }
    }
    return [...deduped.values()];
  }

  private getAlertEmailLevelColor(level: string) {
    const normalized = this.normalizeAlertLevel(level);
    if (normalized === 'CRITICAL') return '#C62828';
    if (normalized === 'WARNING') return '#F57C00';
    return '#1565C0';
  }

  private getAlertEmailLevelLabel(level: string) {
    const normalized = this.normalizeAlertLevel(level);
    if (normalized === 'CRITICAL') return 'Critica';
    if (normalized === 'WARNING') return 'Preventiva';
    return 'Informativa';
  }

  private escapeHtml(value: unknown) {
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  private formatAlertEmailDate(value: unknown) {
    const date = value instanceof Date ? value : new Date(String(value || ''));
    if (Number.isNaN(date.getTime())) return 'No disponible';
    return new Intl.DateTimeFormat('es-EC', {
      dateStyle: 'medium',
      timeStyle: 'short',
      timeZone: 'America/Guayaquil',
    }).format(date);
  }

  private buildAlertConsoleUrl() {
    if (!this.publicBaseUrl) return null;
    return `${this.publicBaseUrl}/alertas`;
  }

  private getInventoryServiceBaseUrl() {
    return String(
      process.env.KPI_INVENTORY_URL || process.env.INVENTORY_SERVICE_URL || '',
    )
      .trim()
      .replace(/\/$/, '');
  }

  private async isInventoryImportRunning() {
    const baseUrl = this.getInventoryServiceBaseUrl();
    if (!baseUrl) return false;
    try {
      const response = await fetch(`${baseUrl}/kardex/import/active/summary`);
      if (!response.ok) {
        this.logger.warn(
          `No se pudo consultar el estado global de importacion de inventario: ${response.status}`,
        );
        return false;
      }
      const payload = (await response.json()) as {
        data?: { active?: boolean };
      };
      return Boolean(payload?.data?.active);
    } catch (error: any) {
      this.logger.warn(
        `No se pudo validar importacion de inventario en curso: ${error?.message ?? 'desconocido'}`,
      );
      return false;
    }
  }

  private getInventoryAlertItems(payload: Record<string, unknown>) {
    return Array.isArray(payload.inventory_items)
      ? payload.inventory_items.filter(
          (item): item is InventoryAlertItem =>
            Boolean(item) && typeof item === 'object',
        )
      : [];
  }

  private buildInventoryAlertTableHtml(payload: Record<string, unknown>) {
    const items = this.getInventoryAlertItems(payload);
    if (!items.length) return '';
    const rows = items
      .map(
        (item) => `
          <tr>
            <td style="padding:10px 12px;border-bottom:1px solid #e6edf5;">${this.escapeHtml(
              item.producto_label,
            )}</td>
            <td style="padding:10px 12px;border-bottom:1px solid #e6edf5;">${this.escapeHtml(
              item.bodega_label,
            )}</td>
            <td style="padding:10px 12px;border-bottom:1px solid #e6edf5;text-align:right;">${this.escapeHtml(
              item.stock_actual.toFixed(2),
            )}</td>
            <td style="padding:10px 12px;border-bottom:1px solid #e6edf5;text-align:right;">${this.escapeHtml(
              item.stock_min_bodega.toFixed(2),
            )}</td>
            <td style="padding:10px 12px;border-bottom:1px solid #e6edf5;">${this.escapeHtml(
              item.observacion,
            )}</td>
          </tr>`,
      )
      .join('');

    return `
      <div style="margin-top:22px;">
        <div style="font-size:13px;color:#5f7388;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:10px;">Materiales afectados</div>
        <div style="border:1px solid #dbe4f0;border-radius:16px;background:#fbfdff;">
          <div style="width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch;border-radius:16px;">
            <table style="width:100%;min-width:760px;border-collapse:collapse;font-size:14px;color:#193550;">
              <thead style="background:#eef4fb;">
                <tr>
                  <th style="padding:12px;text-align:left;">Material</th>
                  <th style="padding:12px;text-align:left;">Bodega</th>
                  <th style="padding:12px;text-align:right;">Stock actual</th>
                  <th style="padding:12px;text-align:right;">Stock minimo</th>
                  <th style="padding:12px;text-align:left;">Observacion</th>
                </tr>
              </thead>
              <tbody>${rows}</tbody>
            </table>
          </div>
        </div>
      </div>
    `;
  }

  private buildInventoryAlertTableText(payload: Record<string, unknown>) {
    const items = this.getInventoryAlertItems(payload);
    if (!items.length) return [];
    return [
      'Materiales afectados:',
      ...items.map(
        (item) =>
          `- ${item.producto_label} | ${item.bodega_label} | actual ${item.stock_actual.toFixed(
            2,
          )} | minimo ${item.stock_min_bodega.toFixed(2)} | ${item.observacion}`,
      ),
    ];
  }

  private buildAlertEmailSubject(
    row: AlertaMantenimientoEntity,
    recipient: AlertNotificationRecipient,
  ) {
    const payload = (row.payload_json ?? {}) as Record<string, unknown>;
    const equipo = this.firstNonEmptyString(
      payload.equipo_codigo,
      payload.equipo_nombre,
      row.equipo_id,
      'General',
    );
    const kind =
      recipient.type === 'TRANSACTION_OWNER'
        ? 'Proceso transaccionado'
        : recipient.type === 'GENERAL_MANAGER'
          ? 'Gerencia general'
          : 'Administrador';
    return `[Alerta ${this.getAlertEmailLevelLabel(row.nivel)}] ${equipo} · ${row.categoria} · ${kind}`;
  }

  private buildAlertEmailHtml(
    row: AlertaMantenimientoEntity,
    recipient: AlertNotificationRecipient,
  ) {
    const payload = (row.payload_json ?? {}) as Record<string, unknown>;
    const accent = this.getAlertEmailLevelColor(row.nivel);
    const recipientLabel =
      recipient.displayName ||
      recipient.username ||
      (recipient.type === 'TRANSACTION_OWNER'
        ? 'equipo operativo'
        : recipient.type === 'GENERAL_MANAGER'
          ? 'gerencia general'
          : 'administracion');
    const equipo = this.firstNonEmptyString(
      payload.equipo_codigo,
      payload.equipo_nombre,
      row.equipo_id,
      'General',
    );
    const consoleUrl = this.buildAlertConsoleUrl();
    const reference =
      row.origen === 'INVENTARIO' && this.getInventoryAlertItems(payload).length
        ? 'Resumen general de inventario'
        : this.firstNonEmptyString(row.referencia, row.referencia_tipo, row.id);
    const inventoryTableHtml = this.buildInventoryAlertTableHtml(payload);

    return `
      <div style="margin:0;padding:24px;background:#f3f6fb;font-family:Arial,sans-serif;color:#15314b;">
        <div style="max-width:760px;margin:0 auto;background:#ffffff;border-radius:18px;overflow:hidden;box-shadow:0 12px 36px rgba(21,49,75,0.12);">
          <div style="padding:28px 32px;background:${accent};color:#ffffff;">
            <div style="font-size:12px;letter-spacing:0.12em;text-transform:uppercase;opacity:0.9;">Justice KPI · Alertas operativas</div>
            <h1 style="margin:10px 0 6px;font-size:26px;line-height:1.2;">${this.escapeHtml(
              row.categoria,
            )} · ${this.escapeHtml(this.getAlertEmailLevelLabel(row.nivel))}</h1>
            <div style="font-size:15px;opacity:0.95;">Se genero una alerta que requiere seguimiento oportuno.</div>
          </div>
          <div style="padding:28px 32px;">
            <p style="margin:0 0 18px;font-size:15px;line-height:1.6;">Hola ${this.escapeHtml(
              recipientLabel,
            )}, el sistema detecto una condicion operativa y te comparte el resumen para que puedas actuar con claridad.</p>
            <div style="border:1px solid #dbe4f0;border-radius:16px;padding:18px 20px;background:#f9fbff;">
              <div style="font-size:13px;color:#5f7388;text-transform:uppercase;letter-spacing:0.08em;">Detalle principal</div>
              <div style="margin-top:10px;font-size:20px;font-weight:700;color:#10263c;">${this.escapeHtml(
                row.detalle || 'Alerta operativa',
              )}</div>
            </div>
            <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-top:18px;">
              ${[
                ['Estado', row.estado],
                ['Origen', row.origen],
                ['Equipo', equipo],
                ['Tipo', row.tipo_alerta],
                ['Referencia', reference],
                ['Generada', this.formatAlertEmailDate(row.fecha_generada)],
              ]
                .map(
                  ([label, value]) => `
                    <div style="border:1px solid #e4ebf3;border-radius:14px;padding:14px 16px;">
                      <div style="font-size:12px;color:#71859b;text-transform:uppercase;letter-spacing:0.08em;">${this.escapeHtml(
                        label,
                      )}</div>
                      <div style="margin-top:8px;font-size:15px;font-weight:600;color:#193550;">${this.escapeHtml(
                        value,
                      )}</div>
                    </div>`,
                )
                .join('')}
            </div>
            ${inventoryTableHtml}
            ${
              consoleUrl
                ? `<div style="margin-top:24px;">
                    <a href="${this.escapeHtml(
                      consoleUrl,
                    )}" style="display:inline-block;padding:13px 22px;border-radius:999px;background:${accent};color:#ffffff;text-decoration:none;font-weight:700;">
                      Abrir modulo de alertas
                    </a>
                  </div>`
                : ''
            }
            <div style="margin-top:24px;padding-top:18px;border-top:1px solid #e7edf5;color:#5f7388;font-size:13px;line-height:1.6;">
              Este correo se envio automaticamente desde el modulo de alertas de mantenimiento para mantener alineados al usuario transaccionante, gerencia general y administracion.
            </div>
          </div>
        </div>
      </div>
    `;
  }

  private buildAlertEmailText(
    row: AlertaMantenimientoEntity,
    recipient: AlertNotificationRecipient,
  ) {
    const payload = (row.payload_json ?? {}) as Record<string, unknown>;
    const equipo = this.firstNonEmptyString(
      payload.equipo_codigo,
      payload.equipo_nombre,
      row.equipo_id,
      'General',
    );
    return [
      `Hola ${recipient.displayName || recipient.username || 'usuario'},`,
      '',
      'Se genero una alerta operativa en Justice KPI.',
      '',
      `Categoria: ${row.categoria}`,
      `Nivel: ${this.getAlertEmailLevelLabel(row.nivel)}`,
      `Estado: ${row.estado}`,
      `Origen: ${row.origen}`,
      `Equipo: ${equipo}`,
      `Tipo: ${row.tipo_alerta}`,
      `Detalle: ${row.detalle || 'Alerta operativa'}`,
      `Referencia: ${
        row.origen === 'INVENTARIO' && this.getInventoryAlertItems(payload).length
          ? 'Resumen general de inventario'
          : this.firstNonEmptyString(row.referencia, row.referencia_tipo, row.id)
      }`,
      `Fecha: ${this.formatAlertEmailDate(row.fecha_generada)}`,
      '',
      ...this.buildInventoryAlertTableText(payload),
      ...(this.getInventoryAlertItems(payload).length ? [''] : []),
      this.buildAlertConsoleUrl()
        ? `Revisa el modulo de alertas en: ${this.buildAlertConsoleUrl()}`
        : '',
    ]
      .filter(Boolean)
      .join('\n');
  }

  private async getAlertMailTransporter() {
    if (this.mailTransporter) return this.mailTransporter;

    const host = String(
      process.env.ALERT_SMTP_HOST ||
        process.env.SMTP_HOST ||
        process.env.MAIL_HOST ||
        '',
    ).trim();
    const port = Number(
      process.env.ALERT_SMTP_PORT ||
        process.env.SMTP_PORT ||
        process.env.MAIL_PORT ||
        587,
    );
    const user = String(
      process.env.ALERT_SMTP_USER ||
        process.env.SMTP_USER ||
        process.env.MAIL_USER ||
        '',
    ).trim();
    const pass = String(
      process.env.ALERT_SMTP_PASS ||
        process.env.SMTP_PASS ||
        process.env.MAIL_PASS ||
        '',
    ).trim();
    const secure = this.coerceBoolean(
      process.env.ALERT_SMTP_SECURE ||
        process.env.SMTP_SECURE ||
        process.env.MAIL_SECURE,
      port === 465,
    );

    if (!host || !port || !this.alertMailFromAddress) {
      return null;
    }

    this.mailTransporter = nodemailer.createTransport({
      host,
      port,
      secure,
      auth: user ? { user, pass } : undefined,
    });

    if (!this.mailTransportVerified) {
      try {
        await this.mailTransporter.verify();
        this.mailTransportVerified = true;
      } catch (error: any) {
        this.logger.warn(
          `No se pudo verificar transporte SMTP de alertas: ${error?.message ?? 'desconocido'}`,
        );
      }
    }

    return this.mailTransporter;
  }

  private async sendAlertTriggerEmails(row: AlertaMantenimientoEntity) {
    const payload = (row.payload_json ?? {}) as Record<string, unknown>;
    const recipients = await this.resolveAlertNotificationRecipients(payload);
    if (!recipients.length) {
      this.logger.warn(
        `[AlertEmail:${row.id}] Sin destinatarios resueltos para la alerta.`,
      );
      return {
        recipients: [],
        userIds: [] as string[],
        recipientTokens: [] as string[],
        sent: [] as string[],
        failed: [] as string[],
        skippedReason: 'No se encontraron destinatarios para la alerta.',
      };
    }

    const transporter = await this.getAlertMailTransporter();
    const sent: string[] = [];
    const failed: string[] = [];
    const transactionOwner = recipients.find(
      (item) => item.type === 'TRANSACTION_OWNER',
    );

    this.logger.log(
      `[AlertEmail:${row.id}] Preparando envio desde=${this.alertMailFromAddress} replyTo=${transactionOwner?.email ?? 'N/A'} destinatarios=${recipients
        .map((item) => `${item.type}:${item.email}`)
        .join(', ')}`,
    );

    if (transporter) {
      for (const recipient of recipients) {
        try {
          await transporter.sendMail({
            from: `"${this.alertMailFromName}" <${this.alertMailFromAddress}>`,
            to: recipient.email,
            replyTo: transactionOwner?.email || undefined,
            subject: this.buildAlertEmailSubject(row, recipient),
            html: this.buildAlertEmailHtml(row, recipient),
            text: this.buildAlertEmailText(row, recipient),
          });
          sent.push(recipient.email);
          this.logger.log(
            `[AlertEmail:${row.id}] Enviado desde=${this.alertMailFromAddress} hacia=${recipient.email} tipo=${recipient.type}`,
          );
        } catch (error: any) {
          failed.push(recipient.email);
          this.logger.warn(
            `[AlertEmail:${row.id}] Fallo envio desde=${this.alertMailFromAddress} hacia=${recipient.email} tipo=${recipient.type}: ${error?.message ?? 'desconocido'}`,
          );
        }
      }
    } else {
      this.logger.warn(
        `[AlertEmail:${row.id}] SMTP no configurado. from=${this.alertMailFromAddress} destinatarios=${recipients
          .map((item) => item.email)
          .join(', ')}`,
      );
    }

    this.logger.log(
      `[AlertEmail:${row.id}] Resultado envio desde=${this.alertMailFromAddress} exitosos=${sent.length} fallidos=${failed.length}`,
    );

    return {
      recipients,
      userIds: this.getAlertNotificationRecipientsUserIds(recipients),
      recipientTokens: this.getAlertNotificationRecipientTokens(recipients),
      sent,
      failed,
      skippedReason: transporter
        ? null
        : 'SMTP no configurado para envio de alertas.',
    };
  }

  private async dispatchAlertTriggeredNotifications(
    row: AlertaMantenimientoEntity,
  ) {
    try {
      const emailResult = await this.sendAlertTriggerEmails(row);
      const inAppRecipients = emailResult.recipientTokens;

      if (inAppRecipients.length) {
        await this.publishInAppNotification({
          title: `Alerta ${this.getAlertEmailLevelLabel(row.nivel)} · ${row.categoria}`,
          body: row.detalle || 'Se genero una nueva alerta operativa.',
          module: 'maintenance',
          entityType: 'alerta',
          entityId: row.id,
          level: this.normalizeAlertLevel(row.nivel).toLowerCase(),
          recipients: inAppRecipients,
        });
      }

      row.payload_json = {
        ...((row.payload_json ?? {}) as Record<string, unknown>),
        alert_notification: {
          triggered_at: new Date().toISOString(),
          recipients: emailResult.recipients.map((item) => ({
            type: item.type,
            email: item.email,
            user_id: item.userId ?? null,
            username: item.username ?? null,
          })),
          email_sent: emailResult.sent,
          email_failed: emailResult.failed,
          in_app_recipients: inAppRecipients,
          in_app_user_ids: emailResult.userIds,
          skipped_reason: emailResult.skippedReason,
        },
      };
      await this.alertaRepo.save(row);

      await this.writeSecurityLog({
        typeLog: 'ALERTA_NOTIFICADA',
        description: `[ALERTA:${row.id}] Notificacion enviada. Exitosas=${emailResult.sent.length}, fallidas=${emailResult.failed.length}`,
      });
    } catch (error: any) {
      this.logger.warn(
        `No se pudieron emitir notificaciones de alerta ${row.id}: ${error?.message ?? 'desconocido'}`,
      );
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
    return this.repairPotentialMojibake(String(value ?? ''))
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-zA-Z0-9]+/g, ' ')
      .trim()
      .toLowerCase();
  }

  private repairPotentialMojibake(value: string) {
    const raw = String(value ?? '');
    if (!/[ÃÂâ€™â€œâ€\uFFFD]/.test(raw)) {
      return raw;
    }

    try {
      const repaired = Buffer.from(raw, 'latin1').toString('utf8');
      const weirdnessScore = (input: string) =>
        (input.match(/[ÃÂâ€™â€œâ€\uFFFD]/g) || []).length;
      return weirdnessScore(repaired) < weirdnessScore(raw) ? repaired : raw;
    } catch {
      return raw;
    }
  }

  private matchesNormalizedToken(source: unknown, target: unknown) {
    const normalizedSource = this.normalizeSearchToken(source);
    const normalizedTarget = this.normalizeSearchToken(target);
    if (!normalizedTarget) return true;
    if (!normalizedSource) return false;
    return (
      normalizedSource === normalizedTarget ||
      normalizedSource.includes(normalizedTarget) ||
      normalizedTarget.includes(normalizedSource)
    );
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
    if (['alerta', 'anormal', 'critico', 'critical', 'rojo'].includes(raw)) {
      return 'ANORMAL';
    }
    if (
      ['observacion', 'precaucion', 'warning', 'warn', 'amarillo'].includes(
        raw,
      )
    ) {
      return 'PRECAUCION';
    }
    if (['n d', 'nd', 'no disponible', 'sin dato', 'sin datos'].includes(raw)) {
      return 'N/D';
    }
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
    if (['anormal', 'alerta', 'critico', 'critical'].includes(raw)) {
      return 'ANORMAL';
    }
    if (['precaucion', 'observacion', 'warning', 'warn'].includes(raw)) {
      return 'PRECAUCION';
    }
    if (['n d', 'nd', 'no disponible', 'sin evaluacion'].includes(raw)) {
      return 'N/D';
    }
    if (['anormal', 'alerta', 'critico', 'critical'].includes(raw)) {
      return 'ANORMAL';
    }
    if (
      ['precaucion', 'observacion', 'warning', 'warn'].includes(raw)
    ) {
      return 'PRECAUCION';
    }
    if (['n d', 'nd', 'no disponible', 'sin evaluacion'].includes(raw)) {
      return 'N/D';
    }
    return 'NORMAL';
  }

  private normalizePositiveNegativeValue(value: unknown) {
    const raw = this.normalizeSearchToken(value);
    if (['positivo', 'positive', 'presente', 'presence'].includes(raw)) {
      return 'POSITIVO';
    }
    if (['negativo', 'negative', 'ausente', 'absent'].includes(raw)) {
      return 'NEGATIVO';
    }
    return null;
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
    const equipoId =
      String(row?.equipo_id ?? sampleInfo.equipo_id ?? payload.equipo_id ?? '').trim() ||
      null;
    const equipoCodigo =
      String(
        row?.equipo_codigo ??
          sampleInfo.equipo_codigo ??
          payload.equipo_codigo ??
          '',
      ).trim() || null;
    const equipoNombre =
      String(
        row?.equipo_nombre ??
          sampleInfo.equipo_nombre ??
          payload.equipo_nombre ??
          '',
      ).trim() || null;
    const equipoModelo =
      String(
        sampleInfo.equipo_modelo ?? payload.equipo_modelo ?? '',
      ).trim() || null;
    const equipoLabel =
      equipoCodigo && equipoNombre && equipoCodigo !== equipoNombre
        ? `${equipoCodigo} · ${equipoNombre}`
        : equipoCodigo || equipoNombre || null;
    const equipoLookupKey = this.normalizeSearchToken(
      [equipoId, equipoCodigo, equipoNombre].filter(Boolean).join(' '),
    );
    const identityLookupKey = this.normalizeSearchToken(
      [
        lubricanteCodigo,
        lubricante,
        marcaLubricante,
        equipoId,
        equipoCodigo,
        equipoNombre,
        equipoModelo,
      ]
        .filter(Boolean)
        .join(' '),
    );

    return {
      lubricante,
      marca_lubricante: marcaLubricante,
      lubricante_codigo: lubricanteCodigo,
      equipo_id: equipoId,
      equipo_codigo: equipoCodigo,
      equipo_nombre: equipoNombre,
      equipo_modelo: equipoModelo,
      equipo_label: equipoLabel,
      equipo_lookup_key: equipoLookupKey,
      identity_lookup_key: identityLookupKey,
      lubricante_label: [lubricanteCodigo, lubricante, marcaLubricante]
        .filter(Boolean)
        .join(' · '),
      identity_label: [
        lubricanteCodigo,
        lubricante,
        marcaLubricante,
        equipoLabel,
        equipoModelo,
      ]
        .filter(Boolean)
        .join(' Â· '),
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
        const normalized = this.normalizePositiveNegativeValue(textValue);
        if (normalized === 'POSITIVO') return 'ANORMAL';
        if (normalized === 'NEGATIVO') return 'NORMAL';
        return 'N/D';
      }
      if (String(definition?.key || '') === 'COMBUSTIBLE') {
        const normalized = this.normalizePositiveNegativeValue(textValue);
        if (normalized === 'POSITIVO') return 'ANORMAL';
        if (normalized === 'NEGATIVO') return 'NORMAL';
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
    equipo_id?: string | null;
    equipo_codigo?: string | null;
    equipo_nombre?: string | null;
    equipo_modelo?: string | null;
  }) {
    const normalizedLubricante = this.normalizeSearchToken(options.lubricante);
    const normalizedCompartimento = this.normalizeSearchToken(
      options.compartimento,
    );
    const normalizedEquipment = this.normalizeSearchToken(
      [
        options.equipo_id,
        options.equipo_codigo,
        options.equipo_nombre,
      ]
        .filter(Boolean)
        .join(' '),
    );
    const normalizedModel = this.normalizeSearchToken(options.equipo_modelo);
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
      if (
        normalizedEquipment &&
        !this.matchesNormalizedToken(identity.equipo_lookup_key, normalizedEquipment)
      ) {
        return false;
      }
      if (
        normalizedModel &&
        this.normalizeSearchToken(identity.equipo_modelo) !== normalizedModel
      ) {
        return false;
      }
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
    equipo_id?: string | null;
    equipo_codigo?: string | null;
    equipo_nombre?: string | null;
    equipo_modelo?: string | null;
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
        const positiveNegativeValue = this.normalizePositiveNegativeValue(rawText);
        if (rawText && !positiveNegativeValue) {
          throw new BadRequestException(
            `El parametro ${definition?.label || detalle.parametro} solo permite NEGATIVO o POSITIVO.`,
          );
        }
        normalizedText = positiveNegativeValue ?? normalizedText;
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
    return this.repairPotentialMojibake(String(value ?? ''))
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
    if (typeof cell?.v === 'string') {
      return this.repairPotentialMojibake(cell.v);
    }
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
    const motorHeaderRow = this.getLubricantSheetHeaderRows(motorSheet)[0] ?? 2;
    const motorLabelColumn = this.getLubricantSheetLabelColumn(
      motorSheet,
      motorHeaderRow,
    );
    const motorValueColumn = this.getLubricantSheetValueColumn(
      motorSheet,
      motorHeaderRow,
      motorLabelColumn,
    );
    const byHeader =
      this.getWorkbookCellText(motorSheet, motorHeaderRow + 1, motorValueColumn) ||
      this.getWorkbookCellText(motorSheet, 3, 20);
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

  private isMeaningfulWorkbookHeaderValue(value: unknown) {
    const raw = String(value ?? '').trim();
    if (!raw) return false;
    const normalized = this.slugifyWorkbookToken(raw);
    if (!normalized) return false;
    return ![
      'COMPARTIMENTO',
      'EQUIPO',
      'MARCA',
      'SERIE',
      'MODELO',
      'LUBRICANTE',
      'MARCADELLUBRICANTE',
      'NOMBREDELCLIENTE',
    ].includes(normalized);
  }

  private getLubricantSheetLabelColumn(
    sheet: XLSX.WorkSheet | undefined,
    headerRow: number,
  ) {
    if (!sheet) return 8;
    const range = XLSX.utils.decode_range(sheet['!ref'] || 'A1:A1');
    const targets = [
      { offset: 0, token: 'COMPARTIMENTO' },
      { offset: 1, token: 'EQUIPO' },
      { offset: 5, token: 'LUBRICANTE' },
      { offset: 6, token: 'MARCADELLUBRICANTE' },
    ];
    let bestColumn = 8;
    let bestScore = -1;

    for (let column = 1; column <= range.e.c + 1; column += 1) {
      let score = 0;
      for (const target of targets) {
        const value = this.slugifyWorkbookToken(
          this.getWorkbookCellText(sheet, headerRow + target.offset, column),
        );
        if (value === target.token) {
          score += 1;
        }
      }
      if (score > bestScore) {
        bestScore = score;
        bestColumn = column;
      }
    }

    return bestColumn;
  }

  private getLubricantSheetValueColumn(
    sheet: XLSX.WorkSheet | undefined,
    headerRow: number,
    labelColumn: number,
  ) {
    if (!sheet) return 20;
    const range = XLSX.utils.decode_range(sheet['!ref'] || 'A1:A1');
    let bestColumn = 20;
    let bestScore = -1;

    for (let column = labelColumn + 1; column <= range.e.c + 1; column += 1) {
      let score = 0;
      for (const rowOffset of [0, 1, 2, 3, 4, 5, 6]) {
        if (
          this.isMeaningfulWorkbookHeaderValue(
            this.getWorkbookCellValue(sheet, headerRow + rowOffset, column),
          )
        ) {
          score += 1;
        }
      }
      if (score > bestScore) {
        bestScore = score;
        bestColumn = column;
      }
    }

    return bestScore > 0 ? bestColumn : 20;
  }

  private getLubricantSheetHeaderRows(sheet: XLSX.WorkSheet | undefined) {
    if (!sheet) return [];
    const range = XLSX.utils.decode_range(sheet['!ref'] || 'A1:A1');
    const starts: number[] = [];

    for (let row = 1; row <= range.e.r + 1; row += 1) {
      for (let column = 1; column <= range.e.c + 1; column += 1) {
        const compartimentoLabel = this.slugifyWorkbookToken(
          this.getWorkbookCellText(sheet, row, column),
        );
        const equipoLabel = this.slugifyWorkbookToken(
          this.getWorkbookCellText(sheet, row + 1, column),
        );
        const lubricanteLabel = this.slugifyWorkbookToken(
          this.getWorkbookCellText(sheet, row + 5, column),
        );
        const marcaLubricanteLabel = this.slugifyWorkbookToken(
          this.getWorkbookCellText(sheet, row + 6, column),
        );

        if (
          compartimentoLabel === 'COMPARTIMENTO' &&
          equipoLabel === 'EQUIPO' &&
          lubricanteLabel === 'LUBRICANTE' &&
          marcaLubricanteLabel === 'MARCADELLUBRICANTE'
        ) {
          starts.push(row);
          break;
        }
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
                ? this.normalizePositiveNegativeValue(textValue)
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
          const labelColumn = this.getLubricantSheetLabelColumn(
            sheet,
            headerRow,
          );
          const valueColumn = this.getLubricantSheetValueColumn(
            sheet,
            headerRow,
            labelColumn,
          );
          const compartimento = this.normalizeImportedCompartment(
            this.getWorkbookCellText(sheet, headerRow, valueColumn) ||
              this.getWorkbookCellText(sheet, headerRow, 20) ||
              sheetName,
          );
          const equipmentHint =
            this.getWorkbookCellText(sheet, headerRow + 1, valueColumn) ||
            this.getWorkbookCellText(sheet, headerRow + 1, 20) ||
            workbookEquipmentHint;
          const equipmentContext = equipmentHint
            ? await this.resolveLubricantImportEquipment(equipmentHint)
            : workbookEquipment;
          const cliente =
            this.getWorkbookCellText(sheet, headerRow + 2, 3) ||
            this.getWorkbookCellText(sheet, headerRow + 2, valueColumn) ||
            'JUSTICE COMPANY';
          const lubricante =
            this.getWorkbookCellText(sheet, headerRow + 5, valueColumn) ||
            this.getWorkbookCellText(sheet, headerRow + 5, 20);
          const marcaLubricante = this.getWorkbookCellText(
            sheet,
            headerRow + 6,
            valueColumn,
          ) || this.getWorkbookCellText(
            sheet,
            headerRow + 6,
            20,
          );
          const serie =
            this.getWorkbookCellText(sheet, headerRow + 3, valueColumn) ||
            this.getWorkbookCellText(sheet, headerRow + 3, 20) ||
            null;
          const modelo =
            this.getWorkbookCellText(sheet, headerRow + 4, valueColumn) ||
            this.getWorkbookCellText(sheet, headerRow + 4, 20) ||
            null;
          const marcaEquipo =
            equipmentContext.marcaNombre ||
            this.getWorkbookCellText(sheet, headerRow + 2, valueColumn) ||
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
                  ? this.normalizePositiveNegativeValue(textValue)
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

  async getAnalisisLubricanteImportTemplate() {
    for (const templatePath of this.lubricantImportTemplatePaths) {
      try {
        const buffer = await readFile(templatePath);
        return {
          filename: 'FORMATO_CARGA_ANALISIS_LUBRICANTE.xlsx',
          buffer,
        };
      } catch {
        // Intenta la siguiente ruta candidata.
      }
    }

    const workbook = XLSX.utils.book_new();
    const rows: (string | number | null)[][] = Array.from(
      { length: 70 },
      () => Array.from({ length: 15 }, () => null),
    );
    const setCell = (row: number, column: number, value: string | number | null) => {
      rows[row - 1][column - 1] = value;
    };

    setCell(1, 1, 'REPORTE DE ANÁLISIS DE LUBRICANTE');
    setCell(2, 3, 'NOMBRE DEL CLIENTE:');
    setCell(2, 8, 'Compartimento:');
    setCell(2, 12, 'MOTOR');
    setCell(3, 8, 'Equipo:');
    setCell(3, 12, 'UG 00');
    setCell(4, 3, 'JUSTICE COMPANY');
    setCell(4, 8, 'Marca:');
    setCell(4, 12, 'MARCA EQUIPO');
    setCell(5, 8, 'Serie:');
    setCell(5, 12, 'SERIE');
    setCell(6, 8, 'Modelo:');
    setCell(6, 12, 'MODELO');
    setCell(7, 8, 'Lubricante:');
    setCell(7, 12, '15W40');
    setCell(8, 8, 'Marca del Lubricante:');
    setCell(8, 12, 'GULF');
    setCell(10, 2, 'INFORMACIÓN DE LA MUESTRA');
    setCell(12, 2, 'Numeración de Muestra');
    setCell(12, 3, 'MUESTRA-001');
    setCell(13, 2, 'Fecha de Muestreo');
    setCell(13, 3, '2026-03-28');
    setCell(14, 2, 'Fecha de Ingreso');
    setCell(14, 3, '2026-03-29');
    setCell(15, 2, 'Fecha de Informe');
    setCell(15, 3, '2026-03-30');
    setCell(16, 2, 'Equipo Hrs/ Km');
    setCell(16, 3, 12000);
    setCell(17, 2, 'Aceite Hrs/ Km');
    setCell(17, 3, 320);
    setCell(18, 2, 'Condición');
    setCell(18, 3, 'NORMAL');
    setCell(20, 2, 'ESTADO DEL LUBRICANTE');
    setCell(30, 2, 'DEGRADACIÓN QUÍMICA');
    setCell(37, 2, 'CONTAMINACIÓN DEL LUBRICANTE - PPM (mg/kg)');
    setCell(44, 2, 'DESGASTE DEL EQUIPO - PPM (mg/kg)');
    setCell(53, 2, 'OTROS ELEMENTOS');
    setCell(61, 2, 'PRESENCIA DE ADITIVOS');

    for (const item of LUBRICANT_IMPORT_PARAMETER_ROWS) {
      const definition = this.getLubricantMetricDefinition(item.label);
      setCell(item.row, 2, definition?.label ?? item.label);
      if (String(definition?.key || '') === 'HUMEDAD') {
        setCell(item.row, 3, 'NEGATIVO');
      } else if (String(definition?.key || '') === 'COMBUSTIBLE') {
        setCell(item.row, 3, 'NEGATIVO');
      } else if (this.lubricantMetricUsesTextResult(definition)) {
        setCell(item.row, 3, 'N/D');
      } else {
        setCell(item.row, 3, 0);
      }
    }

    const sheet = XLSX.utils.aoa_to_sheet(rows);
    sheet['!cols'] = Array.from({ length: 15 }, (_, index) => ({
      wch: index === 1 ? 28 : index === 11 ? 22 : 16,
    }));
    XLSX.utils.book_append_sheet(workbook, sheet, 'MOTOR');

    const instructivoSheet = XLSX.utils.aoa_to_sheet([
      ['INSTRUCTIVO'],
      ['1. Complete la cabecera del análisis en la columna L o en la columna equivalente de valores.'],
      ['2. Cada columna desde la C representa un análisis independiente.'],
      ['3. Cada análisis debe tener al menos una fecha válida en Muestreo, Ingreso o Informe.'],
      ['4. Humedad y Combustible solo aceptan NEGATIVO o POSITIVO.'],
      ['5. Condición solo acepta NORMAL, ANORMAL, PRECAUCION o N/D.'],
      ['6. Puede dejar parámetros vacíos si el laboratorio no reportó valor.'],
    ]);
    instructivoSheet['!cols'] = [{ wch: 100 }];
    XLSX.utils.book_append_sheet(workbook, instructivoSheet, 'Instructivo');

    const buffer = XLSX.write(workbook, {
      type: 'buffer',
      bookType: 'xlsx',
    }) as Buffer;

    return {
      filename: 'FORMATO_CARGA_ANALISIS_LUBRICANTE.xlsx',
      buffer,
    };
  }

  private parseSpanishMonthIndex(value: unknown) {
    const token = this.normalizeWorkbookToken(value);
    const months: Record<string, number> = {
      ENERO: 1,
      FEBRERO: 2,
      MARZO: 3,
      ABRIL: 4,
      MAYO: 5,
      JUNIO: 6,
      JULIO: 7,
      AGOSTO: 8,
      SEPTIEMBRE: 9,
      SETIEMBRE: 9,
      OCTUBRE: 10,
      NOVIEMBRE: 11,
      DICIEMBRE: 12,
    };
    return months[token] ?? null;
  }

  private normalizeProgramacionWorkbookValue(value: unknown) {
    if (value == null) return '';
    if (typeof value === 'string') {
      const repaired = this.repairPotentialMojibake(value);
      return repaired.replace(/\s+/g, ' ').trim();
    }
    if (typeof value === 'number') {
      return Number.isInteger(value) ? String(value) : String(value);
    }
    return this.repairPotentialMojibake(String(value))
      .replace(/\s+/g, ' ')
      .trim();
  }

  private isMeaningfulProgramacionWorkbookValue(value: unknown) {
    const raw = this.normalizeProgramacionWorkbookValue(value);
    if (!raw) return false;
    const normalized = this.normalizeWorkbookToken(raw);
    return !['0', '0.0', 'N/D', 'ND', 'NULL', 'NONE', '-'].includes(normalized);
  }

  private resolveProgramacionMaintenanceDescriptor(value: unknown) {
    const raw = this.normalizeProgramacionWorkbookValue(value);
    const normalized = this.normalizeWorkbookToken(raw);
    if (!raw || !normalized) {
      return {
        raw,
        normalized,
        tipo_mantenimiento: 'VACIO',
        frecuencia_horas: null as number | null,
        es_reportable: false,
      };
    }

    const numericValue = /^\d+([.,]\d+)?$/.test(raw)
      ? Number(raw.replace(/,/g, '.'))
      : null;
    const normalizedNumericValue =
      numericValue != null && Number.isFinite(numericValue)
        ? Math.round(numericValue)
        : null;

    const frequencyMatch = normalized.match(/(250|325|500|650|975|1000|1300)/);
    const frequency =
      frequencyMatch != null
        ? Number(frequencyMatch[1])
        : normalizedNumericValue != null &&
            PROGRAMACION_MPG_FREQUENCIES.has(normalizedNumericValue)
          ? normalizedNumericValue
          : null;
    if (frequency != null && PROGRAMACION_MPG_FREQUENCIES.has(frequency)) {
      return {
        raw,
        normalized: String(frequency),
        tipo_mantenimiento: 'MPG',
        frecuencia_horas: frequency,
        es_reportable: true,
      };
    }

    if (normalizedNumericValue != null && normalizedNumericValue > 0) {
      return {
        raw,
        normalized: String(normalizedNumericValue),
        tipo_mantenimiento: 'HORAS_PROGRAMADAS',
        frecuencia_horas: normalizedNumericValue,
        es_reportable: true,
      };
    }

    const maintenanceKeywords = [
      'R20',
      'FILTRO',
      'FILTROS',
      'OVERHAUL',
      'IZAJE',
      'IN',
      'CHAPAS',
      'BIELA',
      'CONTAMINACION',
      'CONTAMINACIÓN',
      'REPARACION',
      'REPARACIÓN',
      'INSPECCION',
      'INSPECCIÓN',
      'CAMBIO',
      'MPG',
    ];
    const isMaintenanceLike = maintenanceKeywords.some((keyword) =>
      normalized.includes(this.normalizeWorkbookToken(keyword)),
    );

    return {
      raw,
      normalized,
      tipo_mantenimiento: isMaintenanceLike ? 'MANTENIMIENTO' : 'OTRO',
      frecuencia_horas: null as number | null,
      es_reportable: isMaintenanceLike,
    };
  }

  private parseWorkbookTimeRange(value: unknown) {
    const raw = this.normalizeWorkbookToken(value)
      .replace(/\s+/g, '')
      .replace(/H/g, ':');
    const match = raw.match(
      /(\d{1,2}):?(\d{2})\s*-\s*(\d{1,2}):?(\d{2})/,
    );
    if (!match) return null;
    const [, startHour = '0', startMinute = '00', endHour = '0', endMinute = '00'] =
      match;
    const start = `${String(startHour).padStart(2, '0')}:${String(
      startMinute,
    ).padStart(2, '0')}:00`;
    const end = `${String(endHour).padStart(2, '0')}:${String(endMinute).padStart(
      2,
      '0',
    )}:00`;
    const duration =
      (Number(endHour) * 60 +
        Number(endMinute) -
        (Number(startHour) * 60 + Number(startMinute))) /
      60;
    return {
      hora_inicio: start,
      hora_fin: end,
      duracion_horas: Number(duration.toFixed(2)),
    };
  }

  private calculateTimeRangeDurationHours(
    start?: string | null,
    end?: string | null,
  ) {
    const startRaw = String(start || '').slice(0, 5);
    const endRaw = String(end || '').slice(0, 5);
    if (!startRaw || !endRaw) return 0;
    const [startHour = 0, startMinute = 0] = startRaw.split(':').map(Number);
    const [endHour = 0, endMinute = 0] = endRaw.split(':').map(Number);
    const duration =
      (endHour * 60 + endMinute - (startHour * 60 + startMinute)) / 60;
    return Number(Math.max(duration, 0).toFixed(2));
  }

  private resolveProgramacionMensualColorPalette(
    payload?: Record<string, unknown> | null,
  ) {
    const source = (payload ?? {}) as Record<string, unknown>;
    const rawPalette = (source.color_palette ?? {}) as Record<string, unknown>;
    const palette = {
      ...DEFAULT_PROGRAMACION_MONTHLY_COLOR_PALETTE,
    } as Record<string, string>;
    for (const [key, value] of Object.entries(rawPalette)) {
      const normalizedKey = String(key || '').trim().toUpperCase();
      const normalizedValue = String(value || '').trim();
      if (!normalizedKey || !normalizedValue) continue;
      palette[normalizedKey] = normalizedValue;
    }
    return palette;
  }

  private parseCronogramaDateRangeLabel(value: unknown) {
    const raw = String(value ?? '').trim();
    const normalized = this.normalizeWorkbookToken(raw);
    const match = normalized.match(
      /SEMANA DEL\s+(\d{1,2})\s+DE\s+([A-ZÁÉÍÓÚÜ]+)\s+AL\s+(\d{1,2})\s+DE\s+([A-ZÁÉÍÓÚÜ]+)\s+DEL\s+(\d{4})/,
    );
    if (!match) return null;
    const [, fromDay, fromMonthLabel, toDay, toMonthLabel, yearLabel] = match;
    const year = Number(yearLabel);
    const fromMonth = this.parseSpanishMonthIndex(fromMonthLabel);
    const toMonth = this.parseSpanishMonthIndex(toMonthLabel);
    if (!fromMonth || !toMonth || !year) return null;
    const start = new Date(year, fromMonth - 1, Number(fromDay));
    const endYear = toMonth < fromMonth ? year + 1 : year;
    const end = new Date(endYear, toMonth - 1, Number(toDay));
    return {
      fecha_inicio: start.toISOString().slice(0, 10),
      fecha_fin: end.toISOString().slice(0, 10),
      label: raw,
    };
  }

  private resolveCronogramaTipoProceso(value: string) {
    const normalized = this.normalizeWorkbookToken(value);
    const hasSsa = normalized.includes('SSA');
    const hasMpg = normalized.includes('MPG');
    if (hasSsa && hasMpg) return 'MIXTO';
    if (hasMpg) return 'MPG';
    if (hasSsa) return 'SSA';
    return 'OPERACION';
  }

  private extractEquipoCodigoFromText(value: string) {
    const match = this.normalizeWorkbookToken(value).match(
      /UGN?\s*-?\s*0*(\d{1,3})/,
    );
    if (!match) return null;
    return `UG${String(match[1] || '').padStart(2, '0')}`;
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
    const equipos = await this.equipoRepo.find({ where: { is_deleted: false } });
    const detallesEnriquecidos = detalles.map((item) => {
      const matchedEquipment = item.equipo_codigo
        ? this.matchEquipoByCodeOrName(item.equipo_codigo, equipos)
        : null;
      const duracionHoras = this.calculateTimeRangeDurationHours(
        item.hora_inicio,
        item.hora_fin,
      );
      return {
        ...item,
        duracion_horas: duracionHoras,
        equipo_id: matchedEquipment?.id ?? null,
        equipo_nombre: matchedEquipment?.nombre ?? null,
        equipo_codigo: matchedEquipment?.codigo ?? item.equipo_codigo ?? null,
      };
    });
    const dailyHours = detallesEnriquecidos.reduce(
      (acc: Record<string, number>, item) => {
        const key = String(item.fecha_actividad || '');
        if (!key) return acc;
        acc[key] = Number(
          ((acc[key] ?? 0) + this.toNumeric(item.duracion_horas, 0)).toFixed(2),
        );
        return acc;
      },
      {},
    );
    const dailyEquipmentMap = new Map<
      string,
      {
        key: string;
        fecha_actividad: string;
        equipo_id: string | null;
        equipo_codigo: string | null;
        equipo_nombre: string | null;
        total_horas: number;
        total_actividades: number;
        cronograma_ids: string[];
        cronograma_codigos: string[];
        actividades: Array<{
          detalle_id: string;
          actividad: string;
          tipo_proceso: string | null;
          hora_inicio: string | null;
          hora_fin: string | null;
          duracion_horas: number;
          responsable_area: string | null;
          observacion: string | null;
        }>;
      }
    >();
    for (const item of detallesEnriquecidos) {
      const fechaActividad = String(item.fecha_actividad || '').slice(0, 10);
      const equipoCodigo = String(item.equipo_codigo || '').trim();
      if (!fechaActividad || !equipoCodigo) continue;
      const key = `${fechaActividad}::${this.normalizeWorkbookToken(equipoCodigo)}`;
      const current = dailyEquipmentMap.get(key) ?? {
        key,
        fecha_actividad: fechaActividad,
        equipo_id: item.equipo_id ?? null,
        equipo_codigo: item.equipo_codigo ?? null,
        equipo_nombre: item.equipo_nombre ?? null,
        total_horas: 0,
        total_actividades: 0,
        cronograma_ids: [row.id],
        cronograma_codigos: [row.codigo],
        actividades: [],
      };
      current.total_horas = Number(
        (current.total_horas + this.toNumeric(item.duracion_horas, 0)).toFixed(2),
      );
      current.total_actividades += 1;
      current.actividades.push({
        detalle_id: item.id,
        actividad: item.actividad,
        tipo_proceso: item.tipo_proceso ?? null,
        hora_inicio: item.hora_inicio ?? null,
        hora_fin: item.hora_fin ?? null,
        duracion_horas: this.toNumeric(item.duracion_horas, 0),
        responsable_area: item.responsable_area ?? null,
        observacion: item.observacion ?? null,
      });
      dailyEquipmentMap.set(key, current);
    }

    const timeSlots = [...new Set(detallesEnriquecidos.map((item) => `${item.hora_inicio || ''}-${item.hora_fin || ''}`))]
      .filter(Boolean)
      .map((key) => {
        const [hora_inicio = null, hora_fin = null] = key.split('-');
        return {
          key,
          hora_inicio,
          hora_fin,
          label:
            hora_inicio && hora_fin
              ? `${String(hora_inicio).slice(0, 5)} - ${String(hora_fin).slice(0, 5)}`
              : key,
        };
      });

    return {
      ...row,
      daily_hours: dailyHours,
      daily_equipment_hours: [...dailyEquipmentMap.values()].sort((a, b) =>
        `${a.fecha_actividad}-${a.equipo_codigo || ''}`.localeCompare(
          `${b.fecha_actividad}-${b.equipo_codigo || ''}`,
        ),
      ),
      time_slots: timeSlots,
      detalles: detallesEnriquecidos,
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
      const sourcePayload = (row.payload_json ?? {}) as Record<string, unknown>;
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
            programacion_codigo: row.codigo ?? null,
            plan_id: row.plan_id,
            plan_codigo: row.plan_codigo ?? null,
            plan_nombre: row.plan_nombre ?? null,
            procedimiento_id: row.procedimiento_id ?? null,
            procedimiento_codigo: row.procedimiento_codigo ?? null,
            procedimiento_nombre: row.procedimiento_nombre ?? null,
            referencia_label:
              row.procedimiento_nombre ||
              row.plan_nombre ||
              row.plan_codigo ||
              row.codigo ||
              null,
            equipo_id: row.equipo_id ?? null,
            equipo_codigo: row.equipo_codigo ?? null,
            equipo_nombre: row.equipo_nombre ?? null,
            estado_programacion: estado,
            horometro_actual: row.horometro_actual ?? null,
            horas_restantes: hoursRemaining,
            dias_restantes: daysRemaining,
            proxima_horas: row.proxima_horas ?? null,
            proxima_fecha: row.proxima_fecha ?? null,
            actor_user_id: this.firstNonEmptyString(
              sourcePayload.actor_user_id,
              sourcePayload.user_id,
            ),
            actor_username: this.firstNonEmptyString(
              sourcePayload.actor_username,
              sourcePayload.updated_by,
              sourcePayload.created_by,
              sourcePayload.requested_by,
            ),
            actor_email: this.normalizeEmail(
              this.firstNonEmptyString(
                sourcePayload.actor_email,
                sourcePayload.updated_by_email,
                sourcePayload.created_by_email,
                sourcePayload.requested_by_email,
              ),
            ),
            actor_name: this.firstNonEmptyString(sourcePayload.actor_name),
            actor_role: this.firstNonEmptyString(sourcePayload.actor_role),
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
      const reportPayload = ((reporte?.payload_json ?? {}) ||
        {}) as Record<string, unknown>;
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
            actor_username: this.firstNonEmptyString(
              reporte?.updated_by,
              reporte?.created_by,
              reportPayload.actor_username,
              reportPayload.updated_by,
              reportPayload.created_by,
            ),
            actor_email: this.normalizeEmail(
              this.firstNonEmptyString(
                reportPayload.actor_email,
                reportPayload.updated_by_email,
                reportPayload.created_by_email,
              ),
            ),
            actor_user_id: this.firstNonEmptyString(reportPayload.actor_user_id),
            actor_name: this.firstNonEmptyString(reportPayload.actor_name),
            actor_role: this.firstNonEmptyString(reportPayload.actor_role),
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
      const sourcePayload = (row.payload_json ?? {}) as Record<string, unknown>;
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
            actor_username: this.firstNonEmptyString(
              row.updated_by,
              row.created_by,
              sourcePayload.actor_username,
              sourcePayload.updated_by,
              sourcePayload.created_by,
            ),
            actor_email: this.normalizeEmail(
              this.firstNonEmptyString(
                sourcePayload.actor_email,
                sourcePayload.updated_by_email,
                sourcePayload.created_by_email,
              ),
            ),
            actor_user_id: this.firstNonEmptyString(sourcePayload.actor_user_id),
            actor_name: this.firstNonEmptyString(sourcePayload.actor_name),
            actor_role: this.firstNonEmptyString(sourcePayload.actor_role),
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
      const reportPayload = ((reporte?.payload_json ?? {}) ||
        {}) as Record<string, unknown>;
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
            actor_username: this.firstNonEmptyString(
              reporte?.updated_by,
              reporte?.created_by,
              reportPayload.actor_username,
              reportPayload.updated_by,
              reportPayload.created_by,
            ),
            actor_email: this.normalizeEmail(
              this.firstNonEmptyString(
                reportPayload.actor_email,
                reportPayload.updated_by_email,
                reportPayload.created_by_email,
              ),
            ),
            actor_user_id: this.firstNonEmptyString(reportPayload.actor_user_id),
            actor_name: this.firstNonEmptyString(reportPayload.actor_name),
            actor_role: this.firstNonEmptyString(reportPayload.actor_role),
          },
        },
      ];
    });
  }

  private async buildInventoryAlertCandidates(): Promise<AlertCandidate[]> {
    const rows = await this.stockRepo.find({ where: { is_deleted: false } });
    const { productMap, warehouseMap } = await this.buildInventoryCatalogMaps(
      rows.map((row) => row.producto_id),
      rows.map((row) => row.bodega_id),
    );

    const items = rows
      .map((row): InventoryAlertItem | null => {
        const stockActual = this.toNumeric(row.stock_actual);
        const stockMinimo = this.toNumeric(row.stock_min_bodega);
        if (stockMinimo <= 0 || stockActual > stockMinimo) return null;

        const producto = productMap.get(row.producto_id);
        const bodega = warehouseMap.get(row.bodega_id);
        const productoLabel = this.buildProductoLabel(producto) ?? row.producto_id;
        const bodegaLabel = this.buildBodegaLabel(bodega) ?? row.bodega_id;
        const isCritical = stockActual <= 0;

        return {
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
          nivel: isCritical ? 'CRITICAL' : 'WARNING',
          observacion: isCritical
            ? 'Sin stock. Gestionar reposicion inmediata o traslado entre bodegas.'
            : 'Bajo minimo. Revisar reabastecimiento antes de afectar mantenimiento u operacion.',
          actor_username: this.firstNonEmptyString(row.updated_by, row.created_by),
        };
      })
      .filter((item): item is InventoryAlertItem => item !== null)
      .sort((a, b) => {
        const levelDiff = this.alertLevelRank(a.nivel) - this.alertLevelRank(b.nivel);
        if (levelDiff !== 0) return levelDiff;
        return a.producto_label.localeCompare(b.producto_label);
      });

    if (!items.length) return [];

    const criticalCount = items.filter((item) => item.nivel === 'CRITICAL').length;
    const warningCount = items.length - criticalCount;

    return [
      {
        equipo_id: null,
        tipo_alerta: criticalCount > 0 ? 'SIN_STOCK' : 'STOCK_BAJO_BODEGA',
        categoria: 'INVENTARIO' as AlertCategory,
        nivel: criticalCount > 0 ? 'CRITICAL' : 'WARNING',
        origen: 'INVENTARIO' as AlertOrigin,
        referencia_tipo: 'INVENTARIO_RESUMEN',
        referencia: 'INVENTARIO:RESUMEN_GENERAL',
        detalle: `${items.length} material(es) en alerta de inventario.`,
        payload_json: {
          inventory_items: items,
          total_materiales: items.length,
          materiales_criticos: criticalCount,
          materiales_preventivos: warningCount,
          actor_username: this.firstNonEmptyString(
            ...items.map((item) => item.actor_username),
          ),
          actor_email: null,
          actor_user_id: null,
        },
      },
    ];
  }

  private async buildAlertCandidates(options?: { includeInventory?: boolean }) {
    const includeInventory = options?.includeInventory !== false;

    const [programaciones, reportesDiarios, lubricantes, combustibles, inventario] =
      await Promise.all([
        this.buildProgramacionAlertCandidates(),
        this.buildReporteDiarioAlertCandidates(),
        this.buildLubricanteAlertCandidates(),
        this.buildFuelAlertCandidates(),
        includeInventory
          ? this.buildInventoryAlertCandidates()
          : Promise.resolve([] as AlertCandidate[]),
      ]);

    return [
      ...programaciones,
      ...reportesDiarios,
      ...lubricantes,
      ...combustibles,
      ...inventario,
    ];
  }

  private async syncAlertCandidates(
    candidates: AlertCandidate[],
    options?: { managedOrigins?: AlertOrigin[] },
  ) {
    const managedOrigins: AlertOrigin[] = options?.managedOrigins?.length
      ? options.managedOrigins
      : [
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
          ...((existing.payload_json ?? {}) as Record<string, unknown>),
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

      const createdRow = await this.alertaRepo.save(
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
      await this.dispatchAlertTriggeredNotifications(createdRow);
      created += 1;
    }

    const staleRows = [...activeMap.entries()]
      .filter(([key]) => !seen.has(key))
      .map(([, row]) => row)
      .concat(duplicateRows)
      .filter((row) => !this.hasLinkedWorkOrders(row));

    for (const row of staleRows) {
      // For system-managed alerts without linked work orders, closing the
      // condition should leave the record fully closed instead of "resolved".
      // This keeps compatibility with older DB constraints that only accept
      // open/in-process/closed values and matches the inventory use case.
      row.estado = 'CERRADA';
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
        const inventoryItems = this.getInventoryAlertItems(payload);
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
                  [snapshot.code, snapshot.title].filter(Boolean).join(' ? ') ||
                  snapshot.id,
              };
            }

            const nextSnapshot = this.buildAlertWorkOrderSnapshot(persisted);
            return {
              ...nextSnapshot,
              label:
                `${persisted.code} ? ${persisted.title}`.trim() || persisted.id,
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
          ? `${workOrder.code} ? ${workOrder.title}`
          : null;

        const hasClosedWorkOrders =
          linkedWorkOrders.length > 0 &&
          linkedWorkOrders.every(
            (item) =>
              this.normalizeWorkflowStatus(item.status_workflow) === 'CLOSED',
          );
        let referenciaResuelta = this.resolveAlertReferenceDisplay(row, payload);
        if (row.referencia_tipo === 'PROGRAMACION') {
          referenciaResuelta = String(
            payload.procedimiento_nombre ||
              payload.plan_nombre ||
              payload.plan_codigo ||
              referenciaResuelta,
          );
        } else if (row.referencia_tipo === 'REPORTE_DIARIO') {
          referenciaResuelta = String(
            payload.reporte_codigo || payload.fecha_reporte || referenciaResuelta,
          );
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
            .join(' ? ') || referenciaResuelta;
        } else if (row.referencia_tipo === 'INVENTARIO_RESUMEN') {
          referenciaResuelta = 'Resumen general de inventario';
        }

        let title = `${row.tipo_alerta}${equipoLabel ? ` ? ${equipoLabel}` : ''}`;
        let subtitle = String(row.detalle || '').trim();
        let accionSugerida = 'Revisar la condicion y programar la accion correctiva.';

        if (row.origen === 'PROGRAMACION') {
          const planLabel =
            String(
              payload.procedimiento_nombre ||
                payload.plan_nombre ||
                payload.plan_codigo ||
                'Mantenimiento',
            ).trim();
          title = `${planLabel}${equipoLabel ? ` ? ${equipoLabel}` : ''}`;
          accionSugerida =
            row.tipo_alerta === 'MANTENIMIENTO_VENCIDO'
              ? 'Generar o priorizar la OT del mantenimiento vencido.'
              : 'Preparar recursos y abrir la OT preventiva antes del vencimiento.';
        } else if (row.origen === 'REPORTE_DIARIO') {
          const mpg = String(payload.proximo_mpg || '').trim();
          title = `${mpg || 'Seguimiento MPG'}${
            equipoLabel ? ` ? ${equipoLabel}` : ''
          }`;
          accionSugerida =
            row.tipo_alerta === 'REPORTE_DIARIO_VENCIDO'
              ? 'Validar el reporte diario y abrir la intervencion correspondiente.'
              : 'Coordinar la atencion antes de que el equipo quede vencido.';
        } else if (row.origen === 'ANALISIS_LUBRICANTE') {
          const compartimento = String(
            payload.compartimento_principal || 'Compartimento',
          ).trim();
          title = `Analisis de lubricante ? ${compartimento}${
            equipoLabel ? ` ? ${equipoLabel}` : ''
          }`;
          accionSugerida =
            'Revisar diagnostico, tendencias y tomar muestra o intervencion correctiva.';
        } else if (row.origen === 'COMBUSTIBLE') {
          title = `Combustible ? ${String(payload.tanque || 'Tanque').trim()}`;
          accionSugerida =
            'Coordinar abastecimiento y confirmar que el tanque regrese sobre el minimo.';
        } else if (row.origen === 'INVENTARIO') {
          if (inventoryItems.length) {
            const criticalItems = this.toNumeric(payload.materiales_criticos, 0);
            const warningItems = this.toNumeric(
              payload.materiales_preventivos,
              0,
            );
            title = `Inventario ? ${inventoryItems.length} materiales en alerta`;
            subtitle = `Criticos: ${criticalItems} ? Preventivos: ${warningItems}`;
          } else {
            title = [
              'Inventario',
              String(payload.producto_label || '').trim(),
              String(payload.bodega_label || '').trim(),
            ]
              .filter(Boolean)
              .join(' ? ');
          }
          accionSugerida =
            row.tipo_alerta === 'SIN_STOCK'
              ? 'Gestionar reposicion inmediata o traslado entre bodegas.'
              : 'Revisar reabastecimiento antes de afectar mantenimiento u operacion.';
        } else if (row.origen === 'BITACORA') {
          title = `Anomalia de datos${equipoLabel ? ` ? ${equipoLabel}` : ''}`;
          accionSugerida =
            'Validar la bitacora y corregir la lectura antes de continuar.';
        }
        let effectiveEstado = this.normalizeAlertState(row.estado);
        let effectiveNivel = this.normalizeAlertLevel(row.nivel);
        if (hasClosedWorkOrders) {
          const completedWorkOrderLabel =
            workOrderLabel ||
            linkedWorkOrders[linkedWorkOrders.length - 1]?.label ||
            'OT cerrada';
          effectiveEstado = 'CERRADA';
          effectiveNivel = 'INFO';
          if (row.origen === 'PROGRAMACION') {
            subtitle = `Se culminó la OT ${completedWorkOrderLabel}.`;
            accionSugerida =
              'Mantenimiento culminado. Validar actualización de programación y evidencias finales.';
          } else if (!subtitle) {
            subtitle = `Se culminó la OT ${completedWorkOrderLabel}.`;
            accionSugerida = 'Proceso culminado y registrado correctamente.';
          }
        }

        if (!subtitle) {
          subtitle = title;
        }
        if (workOrderLabel && !hasClosedWorkOrders) {
          subtitle = `${subtitle} · OT ${workOrderLabel}`;
        }

        return {
          ...row,
          estado: effectiveEstado,
          nivel: effectiveNivel,
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

  private resolveActorLabel(actor?: RequestActorContext | null) {
    return this.firstNonEmptyString(actor?.displayName, actor?.username);
  }

  private applyWorkOrderAuditStamp(
    workOrder: WorkOrderEntity,
    actor?: RequestActorContext | null,
    mode: 'CREATED' | 'PROCESSED' | 'APPROVED' = 'PROCESSED',
    options?: { action?: string | null; clearApproval?: boolean },
  ) {
    const actorUserId = this.firstNonEmptyString(actor?.userId);
    const actorUsername = this.firstNonEmptyString(actor?.username);
    const actorDisplayName = this.firstNonEmptyString(
      actor?.displayName,
      actor?.username,
    );
    const payload = {
      ...((workOrder.valor_json ?? {}) as Record<string, unknown>),
    };

    if (options?.clearApproval) {
      delete payload.approved_by_user_id;
      delete payload.approved_by_username;
      delete payload.approved_by_name;
      delete payload.approved_at;
      delete payload.approval_action;
    }

    if (!actorUserId && !actorUsername && !actorDisplayName) {
      workOrder.valor_json = payload;
      return;
    }

    const now = new Date().toISOString();
    if (mode === 'CREATED') {
      payload.created_by_user_id =
        actorUserId ?? this.firstNonEmptyString(payload.created_by_user_id);
      payload.created_by_username =
        actorUsername ?? this.firstNonEmptyString(payload.created_by_username);
      payload.created_by_name =
        actorDisplayName ?? this.firstNonEmptyString(payload.created_by_name);
      payload.created_at = payload.created_at ?? now;
    } else if (mode === 'PROCESSED') {
      payload.processed_by_user_id = actorUserId ?? null;
      payload.processed_by_username = actorUsername ?? null;
      payload.processed_by_name = actorDisplayName ?? null;
      payload.processed_at = now;
    } else {
      payload.approved_by_user_id = actorUserId ?? null;
      payload.approved_by_username = actorUsername ?? null;
      payload.approved_by_name = actorDisplayName ?? null;
      payload.approved_at = now;
      payload.approval_action =
        this.firstNonEmptyString(options?.action, payload.approval_action) ?? null;
      workOrder.approved_by = actorUserId ?? workOrder.approved_by ?? null;
    }

    workOrder.valor_json = payload;
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

  private async resolveProgramacionWorkOrder(workOrderId?: string | null) {
    const normalizedId = String(workOrderId || '').trim();
    if (!normalizedId) return null;
    return this.findOneOrFail(this.woRepo, {
      id: normalizedId,
      is_deleted: false,
    });
  }

  private mergeProgramacionWorkOrderPayload(
    payload: Record<string, unknown> | null | undefined,
    workOrder: WorkOrderEntity | null,
  ) {
    const nextPayload = { ...(payload ?? {}) } as Record<string, unknown>;
    if (!workOrder) {
      delete nextPayload.work_order_id;
      delete nextPayload.work_order_code;
      delete nextPayload.work_order_title;
      delete nextPayload.work_order_status;
      return nextPayload;
    }
    nextPayload.work_order_id = workOrder.id;
    nextPayload.work_order_code = workOrder.code ?? null;
    nextPayload.work_order_title = workOrder.title ?? null;
    nextPayload.work_order_status = this.normalizeWorkflowStatus(
      workOrder.status_workflow,
    );
    return nextPayload;
  }

  private collectProgramacionPlannerHints(
    programacion?: ProgramacionPlanEntity | null,
  ) {
    const payload = (programacion?.payload_json ?? {}) as Record<string, unknown>;
    const userIds = new Set<string>();
    const usernames = new Set<string>();
    const addUserId = (...values: unknown[]) => {
      for (const value of values) {
        const normalized = this.firstNonEmptyString(value);
        if (normalized) userIds.add(normalized);
      }
    };
    const addUsername = (...values: unknown[]) => {
      for (const value of values) {
        const normalized = this.normalizeUsername(value);
        if (normalized) usernames.add(normalized);
      }
    };

    addUserId(
      payload.actor_user_id,
      payload.user_id,
      payload.created_by_user_id,
      payload.requested_user_id,
    );
    addUsername(
      payload.actor_username,
      payload.created_by,
      payload.requested_by,
    );

    return {
      userIds,
      usernames,
      ownerLabel:
        this.firstNonEmptyString(
          payload.created_by,
          payload.actor_username,
          payload.requested_by,
        ) || null,
    };
  }

  private async resolveLinkedProgramacionForWorkOrder(workOrderId: string) {
    return this.programacionRepo.findOne({
      where: { work_order_id: workOrderId, is_deleted: false, activo: true },
      order: { proxima_fecha: 'ASC', id: 'ASC' },
    });
  }

  private async canActorCloseOrVoidWorkOrder(
    workOrder: WorkOrderEntity,
    actor?: RequestActorContext | null,
    linkedProgramacion?: ProgramacionPlanEntity | null,
  ) {
    const actorUserId = this.firstNonEmptyString(actor?.userId);
    const actorUsername = this.normalizeUsername(actor?.username);
    if (!actorUserId && !actorUsername) return false;

    const ownerUserIds = new Set<string>();
    const ownerUsernames = new Set<string>();
    const addOwnerUserId = (...values: unknown[]) => {
      for (const value of values) {
        const normalized = this.firstNonEmptyString(value);
        if (normalized) ownerUserIds.add(normalized);
      }
    };
    const addOwnerUsername = (...values: unknown[]) => {
      for (const value of values) {
        const normalized = this.normalizeUsername(value);
        if (normalized) ownerUsernames.add(normalized);
      }
    };

    addOwnerUserId(workOrder.requested_by);
    addOwnerUsername(workOrder.created_by);

    const plannerHints = this.collectProgramacionPlannerHints(linkedProgramacion);
    plannerHints.userIds.forEach((value) => ownerUserIds.add(value));
    plannerHints.usernames.forEach((value) => ownerUsernames.add(value));

    return (
      (!!actorUserId && ownerUserIds.has(actorUserId)) ||
      (!!actorUsername && ownerUsernames.has(actorUsername))
    );
  }

  private async assertCanCloseOrVoidWorkOrder(
    workOrder: WorkOrderEntity,
    actor?: RequestActorContext | null,
    actionLabel = 'cerrar o anular',
  ) {
    const linkedProgramacion = await this.resolveLinkedProgramacionForWorkOrder(
      workOrder.id,
    );
    const allowed = await this.canActorCloseOrVoidWorkOrder(
      workOrder,
      actor,
      linkedProgramacion,
    );
    if (allowed) {
      return { linkedProgramacion, canCloseOrVoid: true };
    }

    const plannerHints = this.collectProgramacionPlannerHints(linkedProgramacion);
    const ownerLabel =
      this.firstNonEmptyString(workOrder.created_by, plannerHints.ownerLabel) ||
      'el usuario que creó o planificó la orden';
    throw new ForbiddenException(
      `Solo ${ownerLabel} puede ${actionLabel} esta orden de trabajo.`,
    );
  }

  private async syncProgramacionWorkOrderLink(
    programacionId: string | null | undefined,
    workOrder: WorkOrderEntity,
  ) {
    const normalizedId = String(programacionId || '').trim();
    if (!normalizedId) return;
    const programacion = await this.programacionRepo.findOne({
      where: { id: normalizedId, is_deleted: false },
    });
    if (!programacion) return;
    programacion.work_order_id = workOrder.id;
    programacion.payload_json = this.mergeProgramacionWorkOrderPayload(
      programacion.payload_json,
      workOrder,
    );
    await this.programacionRepo.save(programacion);
  }

  private async syncProgramacionWorkOrderLinkFromAlert(
    alertaId: string | null | undefined,
    workOrder: WorkOrderEntity,
  ) {
    const normalizedAlertId = String(alertaId || '').trim();
    if (!normalizedAlertId) return;
    const alerta = await this.alertaRepo.findOne({
      where: { id: normalizedAlertId, is_deleted: false },
    });
    if (!alerta) return;
    const payload = (alerta.payload_json ?? {}) as Record<string, unknown>;
    await this.syncProgramacionWorkOrderLink(
      this.firstNonEmptyString(payload.programacion_id),
      workOrder,
    );
  }

  private async detachProgramacionesFromWorkOrder(workOrderId: string) {
    const rows = await this.programacionRepo.find({
      where: { work_order_id: workOrderId, is_deleted: false },
    });
    if (!rows.length) return;
    for (const row of rows) {
      row.work_order_id = null;
      row.payload_json = this.mergeProgramacionWorkOrderPayload(
        row.payload_json,
        null,
      );
    }
    await this.programacionRepo.save(rows);
  }

  private async recalculateProgramacionFields(
    programacion: ProgramacionPlanEntity,
    options?: { persist?: boolean },
  ) {
    const [equipo, plan, linkedWorkOrder] = await Promise.all([
      this.findEquipoOrFail(programacion.equipo_id),
      this.findOneOrFail(this.planRepo, {
        id: programacion.plan_id,
        is_deleted: false,
      }),
      programacion.work_order_id
        ? this.woRepo.findOne({
            where: { id: programacion.work_order_id, is_deleted: false },
          })
        : Promise.resolve(null),
    ]);
    const procedimiento = await this.resolveProcedimientoFromPlan(plan);

    const freqType = String(plan.frecuencia_tipo || 'HORAS').toUpperCase();
    const freqValue = this.toNumeric(plan.frecuencia_valor, 0);
    const patch: Partial<ProgramacionPlanEntity> = {};
    const scheduleMode = String(
      programacion.modo_programacion || 'DINAMICA',
    ).toUpperCase();

    if (scheduleMode !== 'CALENDARIO') {
      if (freqType === 'HORAS') {
        const baseHours =
          programacion.ultima_ejecucion_horas != null
            ? this.toNumeric(programacion.ultima_ejecucion_horas)
            : this.toNumeric(equipo.horometro_actual);
        patch.proxima_horas = Number((baseHours + freqValue).toFixed(2));
        if (
          !programacion.ultima_ejecucion_horas &&
          equipo.horometro_actual != null
        ) {
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
      codigo: programacion.codigo ?? null,
      modo_programacion: scheduleMode,
      origen_programacion:
        programacion.origen_programacion ?? 'MANUAL',
      documento_origen: programacion.documento_origen ?? null,
      payload_json: programacion.payload_json ?? {},
      work_order_id: linkedWorkOrder?.id ?? programacion.work_order_id ?? null,
      work_order_code: linkedWorkOrder?.code ?? null,
      work_order_title: linkedWorkOrder?.title ?? null,
      work_order_status: linkedWorkOrder
        ? this.normalizeWorkflowStatus(linkedWorkOrder.status_workflow)
        : null,
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

  private async enrichWorkOrder(
    workOrder: WorkOrderEntity,
    actor?: RequestActorContext | null,
  ) {
    const [
      equipo,
      plan,
      componente,
      blockingWorkOrder,
      parentWorkOrder,
      linkedProgramacion,
    ] = await Promise.all([
      workOrder.equipment_id
        ? this.equipoRepo.findOne({ where: { id: workOrder.equipment_id, is_deleted: false } })
        : Promise.resolve(null),
      workOrder.plan_id
        ? this.planRepo.findOne({ where: { id: workOrder.plan_id, is_deleted: false } })
        : Promise.resolve(null),
      workOrder.equipo_componente_id
        ? this.equipoComponenteRepo.findOne({
            where: { id: workOrder.equipo_componente_id, is_deleted: false },
          })
        : Promise.resolve(null),
      workOrder.blocked_by_work_order_id
        ? this.woRepo.findOne({
            where: { id: workOrder.blocked_by_work_order_id, is_deleted: false },
          })
        : Promise.resolve(null),
      workOrder.parent_work_order_id
        ? this.woRepo.findOne({
            where: { id: workOrder.parent_work_order_id, is_deleted: false },
          })
        : Promise.resolve(null),
      this.resolveLinkedProgramacionForWorkOrder(workOrder.id),
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

    const plannerHints = this.collectProgramacionPlannerHints(linkedProgramacion);
    const canCloseOrVoid = await this.canActorCloseOrVoidWorkOrder(
      workOrder,
      actor,
      linkedProgramacion,
    );
    const auditPayload = (workOrder.valor_json ?? {}) as Record<string, unknown>;

    return {
      ...workOrder,
      status_workflow: this.normalizeWorkflowStatus(workOrder.status_workflow),
      equipment_nombre: equipo?.nombre ?? null,
      equipment_nombre_real: equipo?.nombre_real ?? null,
      equipment_codigo: equipo?.codigo ?? null,
      plan_nombre: plan?.nombre ?? null,
      plan_codigo: plan?.codigo ?? null,
      procedimiento_id: procedimiento?.id ?? null,
      procedimiento_codigo: procedimiento?.codigo ?? null,
      procedimiento_nombre: procedimiento?.nombre ?? null,
      equipo_componente_id: componente?.id ?? workOrder.equipo_componente_id ?? null,
      equipo_componente_nombre:
        componente?.nombre ?? workOrder.equipo_componente_nombre ?? null,
      equipo_componente_nombre_oficial:
        componente?.nombre_oficial ??
        workOrder.equipo_componente_nombre_oficial ??
        null,
      blocked_by_work_order_id:
        blockingWorkOrder?.id ?? workOrder.blocked_by_work_order_id ?? null,
      blocked_by_work_order_code: blockingWorkOrder?.code ?? null,
      blocked_by_work_order_title: blockingWorkOrder?.title ?? null,
      blocked_by_work_order_status: blockingWorkOrder?.status_workflow ?? null,
      parent_work_order_id:
        parentWorkOrder?.id ?? workOrder.parent_work_order_id ?? null,
      parent_work_order_code: parentWorkOrder?.code ?? null,
      parent_work_order_title: parentWorkOrder?.title ?? null,
      linked_programacion_id: linkedProgramacion?.id ?? null,
      linked_programacion_codigo: linkedProgramacion?.codigo ?? null,
      linked_programacion_owner:
        this.firstNonEmptyString(
          plannerHints.ownerLabel,
          workOrder.created_by,
        ) ?? null,
      created_by_label:
        this.firstNonEmptyString(
          auditPayload.created_by_name,
          auditPayload.created_by_username,
          workOrder.created_by,
        ) ?? null,
      created_by_username:
        this.firstNonEmptyString(
          auditPayload.created_by_username,
          workOrder.created_by,
        ) ?? null,
      created_by_user_id:
        this.firstNonEmptyString(
          auditPayload.created_by_user_id,
          workOrder.requested_by,
        ) ?? null,
      processed_by_label:
        this.firstNonEmptyString(
          auditPayload.processed_by_name,
          auditPayload.processed_by_username,
        ) ?? null,
      processed_by_username:
        this.firstNonEmptyString(auditPayload.processed_by_username) ?? null,
      processed_by_user_id:
        this.firstNonEmptyString(auditPayload.processed_by_user_id) ?? null,
      processed_at: auditPayload.processed_at ?? null,
      approved_by_label:
        this.firstNonEmptyString(
          auditPayload.approved_by_name,
          auditPayload.approved_by_username,
        ) ?? null,
      approved_by_username:
        this.firstNonEmptyString(auditPayload.approved_by_username) ?? null,
      approved_by_user_id:
        this.firstNonEmptyString(
          auditPayload.approved_by_user_id,
          workOrder.approved_by,
        ) ?? null,
      approved_at:
        this.firstNonEmptyString(auditPayload.approved_at) ??
        (workOrder.closed_at ? workOrder.closed_at.toISOString() : null),
      operational_date:
        this.firstNonEmptyString(
          auditPayload.approved_at,
          auditPayload.processed_at,
        ) ??
        (workOrder.closed_at ? workOrder.closed_at.toISOString() : null) ??
        (workOrder.started_at ? workOrder.started_at.toISOString() : null) ??
        (workOrder.created_at ? workOrder.created_at.toISOString() : null),
      approval_action:
        this.firstNonEmptyString(auditPayload.approval_action) ?? null,
      can_close_or_void: canCloseOrVoid,
    };
  }

  private normalizeWorkOrderFilterDateBoundary(
    value: string | undefined,
    edge: 'start' | 'end',
  ) {
    const raw = String(value || '').trim();
    if (!raw) return null;
    const suffix = edge === 'start' ? '00:00:00' : '23:59:59.999';
    return `${raw} ${suffix}`;
  }

  async listEquipos(query: EquipoQueryDto, sucursalId?: string | null) {
    const page = query.page ?? 1;
    const limit = Math.min(query.limit ?? 10, 100);
    const estadoOperativo = query.estado_operativo
      ? this.normalizeEquipoEstadoOperativo(query.estado_operativo)
      : undefined;
    const criticidad = query.criticidad
      ? this.normalizeEquipoCriticidad(query.criticidad)
      : undefined;
    const qb = this.equipoRepo
      .createQueryBuilder('e')
      .leftJoin(
        LocationEntity,
        'location',
        'location.id = e.location_id AND location.is_deleted = false',
      )
      .where('e.is_deleted = false');
    if (sucursalId) {
      qb.andWhere('location.sucursal_id = :sucursalId', { sucursalId });
    }
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
    if (estadoOperativo)
      qb.andWhere('e.estado_operativo = :estado_operativo', {
        estado_operativo: estadoOperativo,
      });
    if (criticidad)
      qb.andWhere('e.criticidad = :criticidad', {
        criticidad,
      });
    const [data, total] = await qb
      .orderBy('e.created_at', 'DESC')
      .skip((page - 1) * limit)
      .take(limit)
      .getManyAndCount();
    return this.wrap(data, 'Equipos listados', { page, limit, total });
  }

  async getEquipo(id: string, sucursalId?: string | null) {
    const row = await this.equipoRepo
      .createQueryBuilder('e')
      .leftJoin(
        LocationEntity,
        'location',
        'location.id = e.location_id AND location.is_deleted = false',
      )
      .where('e.id = :id', { id })
      .andWhere('e.is_deleted = false');
    if (sucursalId) {
      row.andWhere('location.sucursal_id = :sucursalId', { sucursalId });
    }
    const equipo = await row.getOne();
    if (!equipo) {
      throw new NotFoundException('Equipo no encontrado');
    }
    return this.wrap(equipo, 'Equipo obtenido');
  }
  async createEquipo(dto: CreateEquipoDto) {
    const { componentes: _componentes, ...equipoPayload } = dto;
    const criticidad = this.normalizeEquipoCriticidad(
      dto.criticidad ?? EquipoCriticidadEnum.MEDIA,
    );
    const estado_operativo = this.normalizeEquipoEstadoOperativo(
      dto.estado_operativo ?? EquipoEstadoOperativoEnum.OPERATIVO,
    );
    const nombre_real = String(dto.nombre_real ?? '').trim() || null;
    const modelo = String(dto.modelo ?? '').trim() || null;
    const codigo_lubricante =
      String(dto.codigo_lubricante ?? '').trim().toUpperCase() || null;
    const saved = await this.equipoRepo.save(
      this.equipoRepo.create({
        ...equipoPayload,
        criticidad,
        estado_operativo,
        nombre_real,
        modelo,
        codigo_lubricante,
        horometro_actual: dto.horometro_actual ?? 0,
      }),
    );
    if (Array.isArray(dto.componentes)) {
      await this.syncEquipmentComponents(saved.id, dto.componentes);
    } else {
      await this.ensureDefaultEquipmentComponents(saved);
    }
    return this.wrap(saved, 'Equipo creado');
  }
  async updateEquipo(id: string, dto: UpdateEquipoDto) {
    const e = await this.findEquipoOrFail(id);
    const { componentes: _componentes, ...equipoPayload } = dto;
    Object.assign(e, {
      ...equipoPayload,
      criticidad:
        dto.criticidad !== undefined
          ? this.normalizeEquipoCriticidad(dto.criticidad)
          : e.criticidad,
      estado_operativo:
        dto.estado_operativo !== undefined
          ? this.normalizeEquipoEstadoOperativo(dto.estado_operativo)
          : e.estado_operativo,
      nombre_real:
        dto.nombre_real !== undefined
          ? String(dto.nombre_real ?? '').trim() || null
          : e.nombre_real,
      modelo:
        dto.modelo !== undefined ? String(dto.modelo ?? '').trim() || null : e.modelo,
      codigo_lubricante:
        dto.codigo_lubricante !== undefined
          ? String(dto.codigo_lubricante ?? '').trim().toUpperCase() || null
          : e.codigo_lubricante,
    });
    const saved = await this.equipoRepo.save(e);
    if (Array.isArray(dto.componentes)) {
      await this.syncEquipmentComponents(saved.id, dto.componentes);
    }
    return this.wrap(saved, 'Equipo actualizado');
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

  async listLocations(query: LocationQueryDto, sucursalId?: string | null) {
    const page = query.page ?? 1;
    const limit = Math.min(query.limit ?? 10, 100);
    const qb = this.locationRepo
      .createQueryBuilder('l')
      .where('l.is_deleted = false');
    if (sucursalId) {
      qb.andWhere('l.sucursal_id = :sucursalId', { sucursalId });
    }
    if (query.sucursal_id) {
      qb.andWhere('l.sucursal_id = :querySucursalId', {
        querySucursalId: query.sucursal_id,
      });
    }
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

  async getLocation(id: string, sucursalId?: string | null) {
    const qb = this.locationRepo
      .createQueryBuilder('l')
      .where('l.id = :id', { id })
      .andWhere('l.is_deleted = false');
    if (sucursalId) {
      qb.andWhere('l.sucursal_id = :sucursalId', { sucursalId });
    }
    const location = await qb.getOne();
    if (!location) {
      throw new NotFoundException('Location no encontrada');
    }
    return this.wrap(location, 'Location obtenida');
  }

  async createLocation(dto: CreateLocationDto) {
    await this.findOneOrFail(this.sucursalRepo as any, {
      id: dto.sucursal_id,
      is_deleted: false,
    } as any);
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
    await this.findOneOrFail(this.sucursalRepo as any, {
      id: dto.sucursal_id,
      is_deleted: false,
    } as any);
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
      const createdAlert = await this.alertaRepo.save(
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
            actor_username: this.firstNonEmptyString(dto.registrado_por),
          },
          detalle: `Horómetro ${dto.horometro} menor al último ${last.horometro}`,
        }),
      );
      await this.dispatchAlertTriggeredNotifications(createdAlert);
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
    const linkedWorkOrder = await this.resolveProgramacionWorkOrder(
      dto.work_order_id,
    );
    if (!linkedWorkOrder) {
      throw new BadRequestException(
        'Debes vincular la programación a la orden de trabajo que se ejecutará.',
      );
    }
    const resolvedEquipmentId =
      dto.equipo_id ?? linkedWorkOrder?.equipment_id ?? null;
    if (!resolvedEquipmentId) {
      throw new BadRequestException(
        'Debes seleccionar el equipo o la orden de trabajo vinculada.',
      );
    }
    await this.findEquipoOrFail(resolvedEquipmentId);
    let resolvedPlanId = dto.plan_id ?? linkedWorkOrder?.plan_id ?? null;
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
      codigo: dto.codigo?.trim() || null,
      equipo_id: resolvedEquipmentId,
      plan_id: resolvedPlanId,
      work_order_id: linkedWorkOrder?.id ?? null,
      modo_programacion: String(
        dto.modo_programacion || 'DINAMICA',
      ).toUpperCase(),
      origen_programacion: String(
        dto.origen_programacion || 'MANUAL',
      ).toUpperCase(),
      ultima_ejecucion_fecha: dto.ultima_ejecucion_fecha ?? null,
      ultima_ejecucion_horas: dto.ultima_ejecucion_horas ?? null,
      proxima_fecha: dto.proxima_fecha ?? null,
      proxima_horas: dto.proxima_horas ?? null,
      documento_origen: dto.documento_origen ?? null,
      payload_json: this.mergeProgramacionWorkOrderPayload(
        dto.payload_json ?? {},
        linkedWorkOrder,
      ),
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
  async listProgramaciones(sucursalId?: string | null) {
    const rows = await this.programacionRepo.find({
      where: { is_deleted: false, activo: true },
    });
    const scope = await this.buildSucursalScopeContext(sucursalId);
    const scopedRows = !scope
      ? rows
      : rows.filter((row) =>
          scope.equipmentIds.has(String(row.equipo_id || '').trim()),
        );
    const data = await Promise.all(
      scopedRows.map((row) =>
        this.recalculateProgramacionFields(row, { persist: false }),
      ),
    );
    data.sort((a: any, b: any) => {
      const left = a.proxima_fecha || '';
      const right = b.proxima_fecha || '';
      if (left && right) return String(left).localeCompare(String(right));
      return this.toNumeric(a.proxima_horas, 99999999) - this.toNumeric(b.proxima_horas, 99999999);
    });
    return this.wrap(data, 'Programaciones listadas');
  }
  async getProgramacion(id: string, sucursalId?: string | null) {
    const row = await this.findOneOrFail(this.programacionRepo, {
      id,
      is_deleted: false,
    });
    if (sucursalId) {
      const scope = await this.buildSucursalScopeContext(sucursalId);
      if (
        scope &&
        !scope.equipmentIds.has(String(row.equipo_id || '').trim())
      ) {
        throw new NotFoundException('ProgramaciÃ³n no encontrada');
      }
    }
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
    const linkedWorkOrder = await this.resolveProgramacionWorkOrder(
      dto.work_order_id !== undefined ? dto.work_order_id : p.work_order_id,
    );
    if (!linkedWorkOrder) {
      throw new BadRequestException(
        'La programación debe permanecer vinculada a una orden de trabajo.',
      );
    }
    const resolvedEquipmentId =
      dto.equipo_id ?? linkedWorkOrder?.equipment_id ?? p.equipo_id;
    if (!resolvedEquipmentId) {
      throw new BadRequestException(
        'Debes seleccionar el equipo o la orden de trabajo vinculada.',
      );
    }
    await this.findEquipoOrFail(resolvedEquipmentId);
    let resolvedPlanId = dto.plan_id ?? linkedWorkOrder?.plan_id ?? p.plan_id;
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
      codigo: dto.codigo ?? p.codigo ?? null,
      equipo_id: resolvedEquipmentId,
      plan_id: resolvedPlanId,
      work_order_id:
        dto.work_order_id !== undefined
          ? linkedWorkOrder?.id ?? null
          : p.work_order_id ?? null,
      modo_programacion: dto.modo_programacion
        ? String(dto.modo_programacion).toUpperCase()
        : p.modo_programacion ?? 'DINAMICA',
      origen_programacion: dto.origen_programacion
        ? String(dto.origen_programacion).toUpperCase()
        : p.origen_programacion ?? 'MANUAL',
      ultima_ejecucion_fecha: dto.ultima_ejecucion_fecha ?? p.ultima_ejecucion_fecha ?? null,
      ultima_ejecucion_horas: dto.ultima_ejecucion_horas ?? p.ultima_ejecucion_horas ?? null,
      proxima_fecha: dto.proxima_fecha ?? p.proxima_fecha ?? null,
      proxima_horas: dto.proxima_horas ?? p.proxima_horas ?? null,
      documento_origen: dto.documento_origen ?? p.documento_origen ?? null,
      payload_json:
        dto.payload_json !== undefined || dto.work_order_id !== undefined
          ? this.mergeProgramacionWorkOrderPayload(
              dto.payload_json ?? p.payload_json ?? {},
              linkedWorkOrder ?? null,
            )
          : p.payload_json ?? {},
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


  private getDefaultEquipmentComponentTemplates() {
    return [
      {
        codigo: 'RAD',
        nombre: 'Radiador',
        nombre_oficial: 'Radiador (sistema de enfriamiento)',
        categoria: 'ENFRIAMIENTO',
        descripcion:
          'Inspeccion visual, limpieza externa e interna, drenaje, lavado, revision de mangueras, relleno de refrigerante y prueba de temperatura.',
        orden: 10,
      },
      {
        codigo: 'MCI',
        nombre: 'Motor',
        nombre_oficial: 'Motor de combustion interna',
        categoria: 'MOTOR',
        descripcion:
          'Inspeccion mecanica general, cambio de aceite, filtros, inyeccion, correas, baterias y prueba de operacion.',
        orden: 20,
      },
      {
        codigo: 'ALT',
        nombre: 'Alternador',
        nombre_oficial: 'Alternador',
        categoria: 'GENERACION',
        descripcion:
          'Limpieza interna, revision de devanados, aislamiento, rodamientos, conexiones electricas y verificacion de salida.',
        orden: 30,
      },
      {
        codigo: 'CTRL',
        nombre: 'Controlador',
        nombre_oficial: 'Controlador / sistema de control',
        categoria: 'CONTROL',
        descripcion:
          'Revision de conexiones electricas, tarjetas, parametros, alarmas, protecciones y configuracion.',
        orden: 40,
      },
      {
        codigo: 'BMT',
        nombre: 'Barras MT',
        nombre_oficial: 'Barras de media tension',
        categoria: 'DISTRIBUCION',
        descripcion:
          'Desenergizacion, inspeccion visual, limpieza, ajuste de conexiones y pruebas dielectricas o de contacto.',
        orden: 50,
      },
      {
        codigo: 'TRF',
        nombre: 'Transformador',
        nombre_oficial: 'Transformador de potencia',
        categoria: 'POTENCIA',
        descripcion:
          'Inspeccion general, revision del aceite dielectrico, limpieza de aisladores, conexiones y pruebas de aislamiento.',
        orden: 60,
      },
      {
        codigo: 'ARR',
        nombre: 'Arranque',
        nombre_oficial: 'Sistema de arranque',
        categoria: 'ARRANQUE',
        descripcion:
          'Baterias, motor de arranque y cargador de baterias asociados a la unidad de generacion.',
        orden: 70,
      },
      {
        codigo: 'COMB',
        nombre: 'Combustible',
        nombre_oficial: 'Sistema de combustible',
        categoria: 'COMBUSTIBLE',
        descripcion:
          'Tanque diario, lineas, filtros, bombas y control de suministro de combustible.',
        orden: 80,
      },
      {
        codigo: 'LUB',
        nombre: 'Lubricacion',
        nombre_oficial: 'Sistema de lubricacion',
        categoria: 'LUBRICACION',
        descripcion:
          'Carter, bombas, filtros, enfriador y monitoreo del aceite lubricante.',
        orden: 90,
      },
      {
        codigo: 'ADM',
        nombre: 'Admision',
        nombre_oficial: 'Sistema de admision y sobrealimentacion',
        categoria: 'ADMISION',
        descripcion:
          'Filtros de aire, ductos, turboalimentacion e ingreso de aire al motor.',
        orden: 100,
      },
      {
        codigo: 'SENF',
        nombre: 'Enfriamiento',
        nombre_oficial: 'Sistema de enfriamiento',
        categoria: 'ENFRIAMIENTO',
        descripcion:
          'Bomba de agua, termostatos, tuberias, mangueras y circuito de refrigeracion.',
        orden: 110,
      },
    ];
  }

  private normalizeEquipmentComponentName(value: unknown) {
    return String(value ?? '')
      .trim()
      .normalize('NFD')
      .replace(/[̀-ͯ]/g, '')
      .toUpperCase();
  }

  private async ensureDefaultEquipmentComponents(equipo: Pick<EquipoEntity, 'id'>) {
    const existing = await this.equipoComponenteRepo.find({
      where: { equipo_id: equipo.id, is_deleted: false },
    });
    const existingKeys = new Set(
      existing
        .flatMap((item) => [
          this.normalizeEquipmentComponentName(item.codigo),
          this.normalizeEquipmentComponentName(item.nombre),
          this.normalizeEquipmentComponentName(item.nombre_oficial),
        ])
        .filter(Boolean),
    );
    const missing = this.getDefaultEquipmentComponentTemplates().filter(
      (template) =>
        !existingKeys.has(this.normalizeEquipmentComponentName(template.codigo)) &&
        !existingKeys.has(this.normalizeEquipmentComponentName(template.nombre)) &&
        !existingKeys.has(
          this.normalizeEquipmentComponentName(template.nombre_oficial),
        ),
    );
    if (!missing.length) return;
    await this.equipoComponenteRepo.save(
      missing.map((template) =>
        this.equipoComponenteRepo.create({
          equipo_id: equipo.id,
          codigo: template.codigo,
          nombre: template.nombre,
          nombre_oficial: template.nombre_oficial,
          categoria: template.categoria,
          descripcion: template.descripcion,
          orden: template.orden,
        }),
      ),
    );
  }

  private buildEquipmentComponentDrafts(
    components?: Array<Record<string, any>>,
  ) {
    const normalized = (Array.isArray(components) ? components : [])
      .map((component, index) => {
        const codigo = String(component?.codigo ?? '').trim() || null;
        const nombre = String(component?.nombre ?? '').trim();
        const nombreOficial =
          String(component?.nombre_oficial ?? '').trim() || nombre || null;
        const descripcion =
          String(component?.descripcion ?? '').trim() || null;
        const categoria = this.normalizeEquipmentComponentCategory(
          component?.categoria,
        );
        const orden = Number(component?.orden ?? index + 1) || index + 1;
        const hasMeaningfulData = Boolean(
          codigo || nombre || nombreOficial || descripcion || categoria,
        );
        if (!hasMeaningfulData) return null;
        return {
          id: String(component?.id ?? '').trim() || null,
          codigo,
          nombre: nombre || nombreOficial || `Compartimiento ${index + 1}`,
          nombre_oficial: nombreOficial,
          categoria,
          orden,
          descripcion,
        };
      })
      .filter((item): item is NonNullable<typeof item> => Boolean(item));

    return normalized.length
      ? normalized
      : this.getDefaultEquipmentComponentTemplates().map((template) => ({
          id: null,
          codigo: template.codigo,
          nombre: template.nombre,
          nombre_oficial: template.nombre_oficial,
          categoria: template.categoria,
          orden: template.orden,
          descripcion: template.descripcion,
        }));
  }

  private normalizeEquipmentComponentCategory(value: unknown) {
    const normalized = String(value ?? '').trim().toUpperCase();
    return normalized || null;
  }

  private async syncEquipmentComponents(
    equipoId: string,
    components?: Array<Record<string, any>>,
  ) {
    const drafts = this.buildEquipmentComponentDrafts(components);
    const existing = await this.equipoComponenteRepo.find({
      where: { equipo_id: equipoId, is_deleted: false },
      order: { orden: 'ASC' },
    });
    const existingById = new Map(existing.map((item) => [item.id, item]));
    const entitiesToSave: EquipoComponenteEntity[] = [];

    for (const draft of drafts) {
      const entity =
        (draft.id ? existingById.get(draft.id) : null) ??
        this.equipoComponenteRepo.create({
          equipo_id: equipoId,
          status: 'ACTIVE',
        });

      entity.equipo_id = equipoId;
      entity.codigo = draft.codigo;
      entity.nombre = draft.nombre;
      entity.nombre_oficial = draft.nombre_oficial;
      entity.categoria = draft.categoria;
      entity.orden = draft.orden;
      entity.descripcion = draft.descripcion;
      entity.is_deleted = false;
      entitiesToSave.push(entity);
    }

    const saved = await this.equipoComponenteRepo.save(entitiesToSave);
    const retainedIds = new Set(saved.map((item) => item.id));
    const removed = existing.filter((item) => !retainedIds.has(item.id));
    if (removed.length) {
      for (const item of removed) {
        item.is_deleted = true;
      }
      await this.equipoComponenteRepo.save(removed);
    }
  }

  async listComponentes(query: ComponenteQueryDto) {
    const where: FindOptionsWhere<EquipoComponenteEntity> = {
      is_deleted: false,
    };
    if (query.equipo_id) where.equipo_id = query.equipo_id;
    return this.wrap(
      await this.equipoComponenteRepo.find({
        where,
        order: { orden: 'ASC', nombre_oficial: 'ASC', nombre: 'ASC' },
      }),
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
    const payload = {
      ...dto,
      nombre: String(dto.nombre || '').trim(),
      nombre_oficial: String(dto.nombre_oficial || '').trim() || null,
      categoria: this.normalizeEquipmentComponentCategory(dto.categoria),
      orden: Number(dto.orden ?? 1) || 1,
      descripcion: String(dto.descripcion || '').trim() || null,
    };
    return this.wrap(
      await this.equipoComponenteRepo.save(
        this.equipoComponenteRepo.create(payload),
      ),
      'Componente creado',
    );
  }
  async updateComponente(id: string, dto: UpdateComponenteDto) {
    const item = await this.findOneOrFail(this.equipoComponenteRepo, {
      id,
      is_deleted: false,
    });
    Object.assign(item, {
      ...dto,
      nombre: dto.nombre !== undefined ? String(dto.nombre || '').trim() : item.nombre,
      nombre_oficial:
        dto.nombre_oficial !== undefined
          ? String(dto.nombre_oficial || '').trim() || null
          : item.nombre_oficial,
      categoria:
        dto.categoria !== undefined
          ? this.normalizeEquipmentComponentCategory(dto.categoria)
          : item.categoria,
      orden: dto.orden !== undefined ? Number(dto.orden || 1) || 1 : item.orden,
      descripcion:
        dto.descripcion !== undefined
          ? String(dto.descripcion || '').trim() || null
          : item.descripcion,
    });
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

  async listProcedimientosPlantilla(sucursalId?: string | null) {
    const scope = await this.buildSucursalScopeContext(sucursalId);
    const rows = await this.procedimientoRepo.find({
      where: { is_deleted: false },
      order: { updated_at: 'DESC', created_at: 'DESC' },
    });
    return this.wrap(
      await Promise.all(
        rows
          .filter((row) => this.isProcedimientoVisibleForScope(row, scope))
          .map((row) => this.buildProcedimientoPayload(row)),
      ),
      'Procedimientos plantilla listados',
    );
  }

  async getProcedimientoPlantilla(id: string, sucursalId?: string | null) {
    const row = await this.findOneOrFail(this.procedimientoRepo, {
      id,
      is_deleted: false,
    });
    const scope = await this.buildSucursalScopeContext(sucursalId);
    if (!this.isProcedimientoVisibleForScope(row, scope)) {
      throw new NotFoundException('Procedimiento plantilla no encontrado');
    }
    return this.wrap(
      await this.buildProcedimientoPayload(row),
      'Procedimiento plantilla obtenido',
    );
  }

  async createProcedimientoPlantilla(dto: CreateProcedimientoPlantillaDto) {
    await this.ensureInventoryWarehouseExists(dto.bodega_id ?? null);
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
              bodega_id: dto.bodega_id ?? null,
              compartimiento_codigo_referencia:
                this.trimNullableText(dto.compartimiento_codigo_referencia),
              compartimiento_nombre_oficial:
                this.trimNullableText(dto.compartimiento_nombre_oficial),
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
    await this.ensureInventoryWarehouseExists(dto.bodega_id ?? null);
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
        bodega_id: dto.bodega_id ?? row.bodega_id ?? null,
        compartimiento_codigo_referencia:
          dto.compartimiento_codigo_referencia !== undefined
            ? this.trimNullableText(dto.compartimiento_codigo_referencia)
            : row.compartimiento_codigo_referencia ?? null,
        compartimiento_nombre_oficial:
          dto.compartimiento_nombre_oficial !== undefined
            ? this.trimNullableText(dto.compartimiento_nombre_oficial)
            : row.compartimiento_nombre_oficial ?? null,
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

  async listAnalisisLubricante(sucursalId?: string | null) {
    const scope = await this.buildSucursalScopeContext(sucursalId);
    const rows = await this.analisisLubricanteRepo.find({
      where: { is_deleted: false },
      order: { fecha_reporte: 'DESC', created_at: 'DESC' },
    });
    return this.wrap(
      await Promise.all(
        rows
          .filter((row) =>
            this.matchesScopedEquipment(row.equipo_id, row.equipo_codigo, scope),
          )
          .map((row) => this.buildAnalisisLubricantePayload(row)),
      ),
      'Análisis de lubricante listados',
    );
  }

  async getAnalisisLubricante(id: string, sucursalId?: string | null) {
    const row = await this.findOneOrFail(this.analisisLubricanteRepo, {
      id,
      is_deleted: false,
    });
    const scope = await this.buildSucursalScopeContext(sucursalId);
    if (!this.matchesScopedEquipment(row.equipo_id, row.equipo_codigo, scope)) {
      throw new NotFoundException('Análisis de lubricante no encontrado');
    }
    return this.wrap(
      await this.buildAnalisisLubricantePayload(row),
      'Análisis de lubricante obtenido',
    );
  }

  async listAnalisisLubricanteCatalog(
    query: AnalisisLubricanteCatalogQueryDto,
    sucursalId?: string | null,
  ) {
    const scope = await this.buildSucursalScopeContext(sucursalId);
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
        equipo_id: string | null;
        equipo_codigo: string | null;
        equipo_nombre: string | null;
        equipo_modelo: string | null;
        equipo_label: string | null;
        total_analisis: number;
        ultima_fecha_reporte: string | null;
        ultimo_codigo: string | null;
        codigos_analisis: string[];
        clientes: string[];
        compartimentos: string[];
      }
    >();

    for (const row of rows) {
      if (
        !this.matchesScopedEquipment(row.equipo_id, row.equipo_codigo, scope)
      ) {
        continue;
      }
      const identity = this.resolveLubricantIdentity(row);
      if (!identity.identity_lookup_key || !identity.lubricante) continue;
      const existing = catalog.get(identity.identity_lookup_key) ?? {
        key: identity.identity_lookup_key,
        lubricante: identity.lubricante,
        marca_lubricante: identity.marca_lubricante,
        lubricante_codigo: identity.lubricante_codigo,
        equipo_id: identity.equipo_id,
        equipo_codigo: identity.equipo_codigo,
        equipo_nombre: identity.equipo_nombre,
        equipo_modelo: identity.equipo_modelo,
        equipo_label: identity.equipo_label,
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
      catalog.set(identity.identity_lookup_key, existing);
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
            item.equipo_codigo,
            item.equipo_nombre,
            item.equipo_modelo,
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
    sucursalId?: string | null,
  ) {
    const scope = await this.buildSucursalScopeContext(sucursalId);
    const rows = await this.analisisLubricanteRepo.find({
      where: { is_deleted: false },
      order: { fecha_reporte: 'ASC', fecha_muestra: 'ASC', created_at: 'ASC' },
    });
    const scopedRows = rows.filter((row) =>
      this.matchesScopedEquipment(row.equipo_id, row.equipo_codigo, scope),
    );

    const referencedRow = query.codigo
      ? scopedRows.find(
          (item) =>
            this.normalizeSearchToken(item.codigo) ===
            this.normalizeSearchToken(query.codigo),
        ) ?? null
      : null;
    const referenceIdentity = this.resolveLubricantIdentity(referencedRow);
    const targetLookup = this.normalizeSearchToken(
      referenceIdentity.lubricante ?? query.lubricante ?? query.codigo ?? '',
    );
    const targetBrand = this.normalizeSearchToken(
      referenceIdentity.marca_lubricante ?? query.marca_lubricante,
    );
    const targetEquipment = this.normalizeSearchToken(
      [
        query.equipo_id ?? referenceIdentity.equipo_id,
        query.equipo_codigo ?? referenceIdentity.equipo_codigo,
        query.equipo_nombre ?? referenceIdentity.equipo_nombre,
      ]
        .filter(Boolean)
        .join(' '),
    );
    const targetModel = this.normalizeSearchToken(
      query.equipo_modelo ?? referenceIdentity.equipo_modelo,
    );
    const targetCompartimento = this.normalizeSearchToken(query.compartimento);
    const range = this.resolveDashboardDateRange(
      query.periodo,
      query.from,
      query.to,
    );

    const matchingRows = scopedRows.filter((row) => {
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
      if (
        targetEquipment &&
        !this.matchesNormalizedToken(identity.equipo_lookup_key, targetEquipment)
      ) {
        return false;
      }
      if (
        targetModel &&
        this.normalizeSearchToken(identity.equipo_modelo) !== targetModel
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
      equipo_codigo: item.equipo_codigo ?? null,
      equipo_nombre: item.equipo_nombre ?? null,
      equipo_modelo: item.sample_info?.equipo_modelo ?? null,
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
        .map((item) => this.resolveLubricantIdentity(item).identity_lookup_key)
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
                equipo_id: selectedIdentity.equipo_id,
                equipo_codigo: selectedIdentity.equipo_codigo,
                equipo_nombre: selectedIdentity.equipo_nombre,
                equipo_label: selectedIdentity.equipo_label,
                equipo_modelo: selectedIdentity.equipo_modelo,
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
      equipo_id: equipmentContext.equipo?.id ?? dto.equipo_id ?? null,
      equipo_codigo: equipmentContext.equipo?.codigo ?? dto.equipo_codigo ?? null,
      equipo_nombre: equipmentContext.equipo?.nombre ?? dto.equipo_nombre ?? null,
      equipo_modelo: baseSampleInfo.equipo_modelo ?? null,
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
            created_by: this.firstNonEmptyString(
              finalPayloadJson.actor_username,
              finalPayloadJson.created_by,
              finalPayloadJson.updated_by,
            ),
            updated_by: this.firstNonEmptyString(
              finalPayloadJson.actor_username,
              finalPayloadJson.updated_by,
              finalPayloadJson.created_by,
            ),
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
            equipo_id: equipmentContext.equipo?.id ?? dto.equipo_id ?? row.equipo_id ?? null,
            equipo_codigo:
              equipmentContext.equipo?.codigo ?? dto.equipo_codigo ?? row.equipo_codigo ?? null,
            equipo_nombre:
              equipmentContext.equipo?.nombre ?? dto.equipo_nombre ?? row.equipo_nombre ?? null,
            equipo_modelo: mergedSampleInfo.equipo_modelo ?? null,
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
        updated_by: this.firstNonEmptyString(
          finalPayloadJson.actor_username,
          finalPayloadJson.updated_by,
          finalPayloadJson.created_by,
          row.updated_by,
          row.created_by,
        ),
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

  private buildProgramacionMensualColumnMap(sheet: XLSX.WorkSheet | undefined) {
    if (!sheet) return [] as Array<{ column: number; date: string; day: number }>;
    const range = XLSX.utils.decode_range(sheet['!ref'] || 'A1:A1');
    const columns: Array<{ column: number; date: string; day: number }> = [];
    let currentYear: number | null = null;
    let currentMonth: number | null = null;

    for (let column = 4; column <= range.e.c + 1; column += 1) {
      const monthCellValue = this.getWorkbookCellValue(sheet, 1, column);
      const explicitDate = this.safeDateOnlyString(monthCellValue);
      if (explicitDate) {
        const parsed = new Date(explicitDate);
        currentYear = parsed.getUTCFullYear();
        currentMonth = parsed.getUTCMonth() + 1;
      } else {
        const monthIndex = this.parseSpanishMonthIndex(monthCellValue);
        if (monthIndex) {
          if (currentMonth != null && currentYear != null && monthIndex < currentMonth) {
            currentYear += 1;
          } else if (currentYear == null) {
            currentYear = new Date().getUTCFullYear();
          }
          currentMonth = monthIndex;
        }
      }

      const dayValue = this.getWorkbookCellValue(sheet, 2, column);
      const day =
        typeof dayValue === 'number'
          ? Math.trunc(dayValue)
          : Number(String(dayValue ?? '').trim());
      if (
        !currentYear ||
        !currentMonth ||
        !Number.isFinite(day) ||
        day <= 0 ||
        day > 31
      ) {
        continue;
      }

      const date = new Date(Date.UTC(currentYear, currentMonth - 1, day));
      columns.push({
        column,
        date: date.toISOString().slice(0, 10),
        day,
      });
    }

    return columns;
  }

  private isProgramacionMensualEquipmentRow(
    sheet: XLSX.WorkSheet | undefined,
    rowNumber: number,
  ) {
    const tag = this.normalizeWorkbookToken(
      this.getWorkbookCellValue(sheet, rowNumber, 1),
    );
    if (!/^UGN?\d+$/.test(tag.replace(/[^A-Z0-9]/g, ''))) return false;
    const second = this.normalizeWorkbookToken(
      this.getWorkbookCellValue(sheet, rowNumber, 2),
    );
    const third = this.normalizeWorkbookToken(
      this.getWorkbookCellValue(sheet, rowNumber, 3),
    );
    if (second.startsWith('UG') || third.startsWith('UG')) return false;
    return true;
  }

  private matchEquipoByCodeOrName(
    hint: string,
    equipos: Pick<EquipoEntity, 'id' | 'codigo' | 'nombre'>[],
  ) {
    const normalizedHint = this.normalizeWorkbookToken(hint).replace(
      /[^A-Z0-9]/g,
      '',
    );
    const numericHint = normalizedHint.replace(/[^0-9]/g, '');
    return (
      equipos.find((item) => {
        const code = this.normalizeWorkbookToken(item.codigo).replace(
          /[^A-Z0-9]/g,
          '',
        );
        const name = this.normalizeWorkbookToken(item.nombre).replace(
          /[^A-Z0-9]/g,
          '',
        );
        const numericCode = code.replace(/[^0-9]/g, '');
        return (
          code === normalizedHint ||
          name === normalizedHint ||
          (numericHint && numericCode === numericHint)
        );
      }) ?? null
    );
  }

  private async resolveProgramacionMensualProcedure(
    rawValue: string,
    frecuenciaHoras: number | null,
    procedimientos: ProcedimientoPlantillaEntity[],
    syncCache: Map<string, { plan: PlanMantenimientoEntity; procedimiento: ProcedimientoPlantillaEntity }>,
  ) {
    const rawToken = this.normalizeSearchToken(rawValue);
    let procedure =
      frecuenciaHoras != null
        ? procedimientos.find(
            (item) => Number(item.frecuencia_horas || 0) === frecuenciaHoras,
          ) ?? null
        : null;

    if (!procedure && rawToken) {
      procedure =
        procedimientos.find((item) => {
          const haystack = this.normalizeSearchToken(
            `${item.codigo || ''} ${item.nombre || ''} ${
              item.clase_mantenimiento || ''
            } ${item.tipo_proceso || ''}`,
          );
          return (
            haystack === rawToken ||
            haystack.includes(rawToken) ||
            rawToken.includes(haystack)
          );
        }) ?? null;
    }

    if (!procedure) {
      return {
        procedimiento_id: null as string | null,
        plan_id: null as string | null,
        es_sincronizable: false,
      };
    }

    let synced = syncCache.get(procedure.id);
    if (!synced) {
      synced = await this.syncPlanFromProcedimiento(procedure.id);
      syncCache.set(procedure.id, synced);
    }

    return {
      procedimiento_id: procedure.id,
      plan_id: synced.plan.id,
      es_sincronizable: true,
    };
  }

  private async parseProgramacionMensualWorkbook(
    buffer: Buffer,
    fileName: string,
  ): Promise<ParsedProgramacionMensualWorkbook> {
    const workbook = XLSX.read(buffer, {
      type: 'buffer',
      cellDates: true,
      cellStyles: true,
    });
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName || ''];
    if (!sheet) {
      throw new BadRequestException(
        'El archivo mensual no contiene una hoja válida para procesar.',
      );
    }

    const columns = this.buildProgramacionMensualColumnMap(sheet);
    if (!columns.length) {
      throw new BadRequestException(
        'No se pudieron identificar columnas de calendario válidas en el Excel mensual.',
      );
    }

    const equipos = await this.equipoRepo.find({ where: { is_deleted: false } });
    const procedimientos = await this.procedimientoRepo.find({
      where: { is_deleted: false },
    });
    const syncCache = new Map<
      string,
      { plan: PlanMantenimientoEntity; procedimiento: ProcedimientoPlantillaEntity }
    >();
    const warnings: string[] = [];
    const details: ParsedProgramacionMensualWorkbook['detalles'] = [];
    const range = XLSX.utils.decode_range(sheet['!ref'] || 'A1:A1');

    let order = 1;
    for (let rowNumber = 3; rowNumber <= range.e.r + 1; rowNumber += 1) {
      if (!this.isProgramacionMensualEquipmentRow(sheet, rowNumber)) continue;
      const equipoCodigo = this.normalizeProgramacionWorkbookValue(
        this.getWorkbookCellValue(sheet, rowNumber, 1),
      );
      const horometroUltimo = this.getWorkbookCellNumber(sheet, rowNumber, 2);
      const horometroActual = this.getWorkbookCellNumber(sheet, rowNumber, 3);
      const equipo = this.matchEquipoByCodeOrName(equipoCodigo, equipos);
      if (!equipo) {
        warnings.push(
          `No se encontró un equipo del sistema para la fila ${rowNumber} (${equipoCodigo}).`,
        );
      }

      for (const column of columns) {
        const rawValue = this.getWorkbookCellValue(sheet, rowNumber, column.column);
        if (!this.isMeaningfulProgramacionWorkbookValue(rawValue)) continue;
        const descriptor = this.resolveProgramacionMaintenanceDescriptor(rawValue);
        if (!descriptor.es_reportable) continue;
        const horometroProgramado =
          descriptor.frecuencia_horas != null && horometroUltimo != null
            ? Number((horometroUltimo + descriptor.frecuencia_horas).toFixed(2))
            : null;

        const mapping = await this.resolveProgramacionMensualProcedure(
          descriptor.raw,
          descriptor.frecuencia_horas,
          procedimientos,
          syncCache,
        );

        details.push({
          equipo_id: equipo?.id ?? null,
          equipo_codigo: String(equipo?.codigo || equipoCodigo).trim(),
          equipo_nombre: String(equipo?.nombre || '').trim() || null,
          fecha_programada: column.date,
          dia_mes: column.day,
          valor_crudo: descriptor.raw,
          valor_normalizado: descriptor.normalized,
          tipo_mantenimiento: descriptor.tipo_mantenimiento,
          frecuencia_horas: descriptor.frecuencia_horas,
          procedimiento_id: mapping.procedimiento_id,
          plan_id: mapping.plan_id,
          es_sincronizable:
            Boolean(mapping.es_sincronizable) && Boolean(equipo?.id),
          observacion: null,
          orden: order,
        payload_json: {
          hoja_origen: sheetName,
          fila_excel: rowNumber,
          columna_excel: column.column,
          horometro_ultimo: horometroUltimo,
          horometro_actual: horometroActual,
          horometro_programado: horometroProgramado,
          horas_programadas: descriptor.frecuencia_horas ?? null,
          color_key: descriptor.tipo_mantenimiento,
        },
      });
        order += 1;
      }
    }

    if (!details.length) {
      throw new BadRequestException(
        'El archivo no contiene mantenimientos mensuales válidos para importar.',
      );
    }

    const dates = details
      .map((item) => item.fecha_programada)
      .filter(Boolean)
      .sort((a, b) => String(a).localeCompare(String(b)));

    return {
      header: {
        codigo: await this.generateNextProgramacionMensualCode(),
        fecha_inicio: dates[0] ?? null,
        fecha_fin: dates[dates.length - 1] ?? null,
        documento_origen: fileName,
        nombre_archivo: fileName,
        resumen: `Importación mensual MPG desde ${fileName}`,
        payload_json: {
          hoja_origen: sheetName,
          total_detalles: details.length,
          columnas_calendario: columns.length,
          color_palette: {
            ...DEFAULT_PROGRAMACION_MONTHLY_COLOR_PALETTE,
          },
        },
      },
      detalles: details,
      warnings,
    };
  }

  private async upsertCalendarProgramacionFromMonthlyDetail(payload: {
    equipo_id: string;
    plan_id: string;
    fecha_programada: string;
    documento_origen: string;
    valor_crudo: string;
    frecuencia_horas?: number | null;
    ultima_ejecucion_horas?: number | null;
    proxima_horas?: number | null;
    payload_json?: Record<string, unknown>;
  }) {
    const existing = await this.programacionRepo.findOne({
      where: {
        equipo_id: payload.equipo_id,
        plan_id: payload.plan_id,
        proxima_fecha: payload.fecha_programada,
        modo_programacion: 'CALENDARIO',
        is_deleted: false,
      },
    });

    if (existing) {
      existing.origen_programacion = 'MENSUAL_IMPORT';
      existing.documento_origen = payload.documento_origen;
      existing.proxima_fecha = payload.fecha_programada;
      existing.ultima_ejecucion_horas =
        payload.ultima_ejecucion_horas ?? existing.ultima_ejecucion_horas;
      existing.proxima_horas = payload.proxima_horas ?? existing.proxima_horas;
      existing.payload_json = {
        ...(existing.payload_json ?? {}),
        ...(payload.payload_json ?? {}),
        valor_crudo: payload.valor_crudo,
        frecuencia_horas: payload.frecuencia_horas ?? null,
      };
      existing.activo = true;
      return this.programacionRepo.save(existing);
    }

    return this.programacionRepo.save(
      this.programacionRepo.create({
        codigo: null,
        equipo_id: payload.equipo_id,
        plan_id: payload.plan_id,
        modo_programacion: 'CALENDARIO',
        origen_programacion: 'MENSUAL_IMPORT',
        ultima_ejecucion_fecha: null,
        ultima_ejecucion_horas: payload.ultima_ejecucion_horas ?? null,
        proxima_fecha: payload.fecha_programada,
        proxima_horas: payload.proxima_horas ?? null,
        documento_origen: payload.documento_origen,
        payload_json: {
          ...(payload.payload_json ?? {}),
          valor_crudo: payload.valor_crudo,
          frecuencia_horas: payload.frecuencia_horas ?? null,
        },
        activo: true,
      }),
    );
  }

  private resolveProgramacionMensualRange(
    row: ProgramacionMensualEntity,
    details: ProgramacionMensualDetalleEntity[],
    query?: ProgramacionMensualQueryDto,
  ) {
    if (query?.periodo) {
      const [year, month] = String(query.periodo).split('-').map(Number);
      if (year && month) {
        const start = `${query.periodo}-01`;
        const end = new Date(Date.UTC(year, month, 0))
          .toISOString()
          .slice(0, 10);
        return { start, end };
      }
    }
    const orderedDates = details
      .map((item) => String(item.fecha_programada || '').slice(0, 10))
      .filter(Boolean)
      .sort((a, b) => a.localeCompare(b));
    return {
      start:
        this.toDateOnlyString(row.fecha_inicio) ??
        orderedDates[0] ??
        null,
      end:
        this.toDateOnlyString(row.fecha_fin) ??
        orderedDates[orderedDates.length - 1] ??
        null,
    };
  }

  private async buildProgramacionMensualWeeklyAggregates(
    programacionMensualId: string,
    startDate?: string | null,
    endDate?: string | null,
  ) {
    if (!startDate || !endDate) return [];
    const weeklyRows = await this.cronogramaSemanalRepo.find({
      where: {
        is_deleted: false,
      },
      order: { fecha_inicio: 'ASC', created_at: 'ASC' },
    });
    const overlapping = weeklyRows.filter((row) => {
      const start = this.toDateOnlyString(row.fecha_inicio);
      const end = this.toDateOnlyString(row.fecha_fin);
      if (!start || !end) return false;
      return start <= endDate && end >= startDate;
    });
    if (!overlapping.length) return [];
    const cronogramaIds = overlapping.map((item) => item.id);
    const scheduleById = new Map(overlapping.map((item) => [item.id, item]));
    const weeklyDetails = await this.cronogramaSemanalDetRepo.find({
      where: {
        cronograma_id: In(cronogramaIds),
        is_deleted: false,
      },
      order: { fecha_actividad: 'ASC', hora_inicio: 'ASC', orden: 'ASC' },
    });
    const filtered = weeklyDetails.filter((item) => {
      const fecha = this.toDateOnlyString(item.fecha_actividad);
      return Boolean(
        fecha &&
          fecha >= startDate &&
          fecha <= endDate &&
          String(item.equipo_codigo || '').trim(),
      );
    });
    if (!filtered.length) return [];
    const equipos = await this.equipoRepo.find({ where: { is_deleted: false } });
    const aggregates = new Map<
      string,
      {
        id: string;
        programacion_mensual_id: string;
        programacion_id: null;
        equipo_id: string | null;
        equipo_codigo: string | null;
        equipo_nombre: string | null;
        fecha_programada: string;
        dia_mes: number | null;
        valor_crudo: string;
        valor_normalizado: string;
        tipo_mantenimiento: string;
        frecuencia_horas: null;
        procedimiento_id: null;
        plan_id: null;
        es_sincronizable: false;
        observacion: string | null;
        orden: number;
        payload_json: Record<string, unknown>;
      }
    >();

    for (const item of filtered) {
      const fecha = this.toDateOnlyString(item.fecha_actividad);
      const equipo = this.matchEquipoByCodeOrName(
        String(item.equipo_codigo || ''),
        equipos,
      );
      const equipoCodigo = String(equipo?.codigo || item.equipo_codigo || '').trim();
      if (!fecha || !equipoCodigo) continue;
      const duration = this.calculateTimeRangeDurationHours(
        item.hora_inicio,
        item.hora_fin,
      );
      const key = `${fecha}::${this.normalizeWorkbookToken(equipoCodigo)}`;
      const current = aggregates.get(key) ?? {
        id: `weekly:${key}`,
        programacion_mensual_id: programacionMensualId,
        programacion_id: null,
        equipo_id: equipo?.id ?? null,
        equipo_codigo: equipoCodigo,
        equipo_nombre: equipo?.nombre ?? null,
        fecha_programada: fecha,
        dia_mes: Number(fecha.slice(-2)),
        valor_crudo: '0.00 h',
        valor_normalizado: '0.00 H',
        tipo_mantenimiento: 'SEMANAL',
        frecuencia_horas: null,
        procedimiento_id: null,
        plan_id: null,
        es_sincronizable: false as const,
        observacion: null,
        orden: 9999,
        payload_json: {
          fuente_programacion: 'SEMANAL',
          color_key: 'SEMANAL',
          total_horas_agendadas: 0,
          total_actividades: 0,
          cronograma_ids: [],
          cronograma_codigos: [],
          weekly_items: [],
        },
      };
      const nextTotal = Number(
        (
          this.toNumeric(current.payload_json.total_horas_agendadas, 0) + duration
        ).toFixed(2),
      );
      const nextActivities =
        this.toNumeric(current.payload_json.total_actividades, 0) + 1;
      const cronograma = scheduleById.get(item.cronograma_id);
      const cronogramaIds = Array.isArray(current.payload_json.cronograma_ids)
        ? [...(current.payload_json.cronograma_ids as string[])]
        : [];
      const cronogramaCodigos = Array.isArray(current.payload_json.cronograma_codigos)
        ? [...(current.payload_json.cronograma_codigos as string[])]
        : [];
      if (cronograma?.id && !cronogramaIds.includes(cronograma.id)) {
        cronogramaIds.push(cronograma.id);
      }
      if (cronograma?.codigo && !cronogramaCodigos.includes(cronograma.codigo)) {
        cronogramaCodigos.push(cronograma.codigo);
      }
      const weeklyItems = Array.isArray(current.payload_json.weekly_items)
        ? [...(current.payload_json.weekly_items as Record<string, unknown>[])]
        : [];
      weeklyItems.push({
        detalle_id: item.id,
        cronograma_id: item.cronograma_id,
        cronograma_codigo: cronograma?.codigo ?? null,
        actividad: item.actividad,
        tipo_proceso: item.tipo_proceso ?? null,
        hora_inicio: item.hora_inicio ?? null,
        hora_fin: item.hora_fin ?? null,
        responsable_area: item.responsable_area ?? null,
        observacion: item.observacion ?? null,
        duracion_horas: duration,
      });
      current.valor_crudo = `${nextTotal.toFixed(2)} h`;
      current.valor_normalizado = `${nextTotal.toFixed(2)} H`;
      current.observacion = `${nextActivities} actividad(es) semanal(es) para ${equipoCodigo}`;
      current.payload_json = {
        ...current.payload_json,
        total_horas_agendadas: nextTotal,
        total_actividades: nextActivities,
        cronograma_ids: cronogramaIds,
        cronograma_codigos: cronogramaCodigos,
        weekly_items: weeklyItems,
      };
      aggregates.set(key, current);
    }

    return [...aggregates.values()].sort((a, b) =>
      `${a.fecha_programada}-${a.equipo_codigo || ''}`.localeCompare(
        `${b.fecha_programada}-${b.equipo_codigo || ''}`,
      ),
    );
  }

  private async buildProgramacionMensualPayload(
    row: ProgramacionMensualEntity,
    query?: ProgramacionMensualQueryDto,
  ) {
    const details = await this.programacionMensualDetRepo.find({
      where: { programacion_mensual_id: row.id, is_deleted: false },
      order: { fecha_programada: 'ASC', orden: 'ASC', created_at: 'ASC' },
    });
    const { start, end } = this.resolveProgramacionMensualRange(row, details, query);
    const weeklyAggregates = await this.buildProgramacionMensualWeeklyAggregates(
      row.id,
      start,
      end,
    );
    const combinedDetails = [...details, ...weeklyAggregates];
    const periodsMap = new Map<
      string,
      { period: string; total: number; sincronizados: number; label: string }
    >();
    for (const detail of combinedDetails) {
      const period = String(detail.fecha_programada || '').slice(0, 7);
      if (!period) continue;
      const parsed = new Date(`${period}-01T00:00:00Z`);
      const label = parsed.toLocaleDateString('es-EC', {
        month: 'long',
        year: 'numeric',
        timeZone: 'UTC',
      });
      const current = periodsMap.get(period) ?? {
        period,
        total: 0,
        sincronizados: 0,
        label,
      };
      current.total += 1;
      if (detail.programacion_id) current.sincronizados += 1;
      periodsMap.set(period, current);
    }

    const filteredDetails = query?.periodo
      ? details.filter((item) =>
          String(item.fecha_programada || '').startsWith(query.periodo || ''),
        )
      : details;
    const filteredCombinedDetails = query?.periodo
      ? combinedDetails.filter((item) =>
          String(item.fecha_programada || '').startsWith(query.periodo || ''),
        )
      : combinedDetails;
    const palette = this.resolveProgramacionMensualColorPalette(
      (row.payload_json ?? {}) as Record<string, unknown>,
    );

    return {
      ...row,
      total_detalles: details.length,
      total_detalles_consolidados: combinedDetails.length,
      periodos: [...periodsMap.values()].sort((a, b) =>
        String(a.period).localeCompare(String(b.period)),
      ),
      detalles: filteredDetails,
      detalles_consolidados: filteredCombinedDetails,
      color_palette: palette,
    };
  }

  private async disableProgramacionLink(programacionId?: string | null) {
    if (!programacionId) return;
    const row = await this.programacionRepo.findOne({
      where: { id: programacionId, is_deleted: false },
    });
    if (!row) return;
    row.activo = false;
    row.origen_programacion = 'MENSUAL_EDIT';
    await this.programacionRepo.save(row);
  }

  private async prepareProgramacionMensualDetailInput(
    dto: CreateProgramacionMensualDetalleDto | UpdateProgramacionMensualDetalleDto,
    current?: ProgramacionMensualDetalleEntity | null,
  ) {
    const rawValue = this.normalizeProgramacionWorkbookValue(
      dto.valor_crudo ?? current?.valor_crudo ?? '',
    );
    if (!rawValue) {
      throw new BadRequestException(
        'Debes indicar el valor mensual a programar, por ejemplo 325, 650, 975, R20 o una cantidad de horas.',
      );
    }
    const equipos = await this.equipoRepo.find({ where: { is_deleted: false } });
    let equipo: Pick<EquipoEntity, 'id' | 'codigo' | 'nombre'> | null =
      dto.equipo_id != null
        ? await this.findEquipoOrFail(dto.equipo_id)
        : null;
    if (!equipo && dto.equipo_codigo) {
      equipo =
        this.matchEquipoByCodeOrName(
          String(dto.equipo_codigo || ''),
          equipos,
        ) ?? null;
    }
    if (!equipo && current?.equipo_id) {
      equipo = await this.findEquipoOrFail(current.equipo_id);
    }
    if (!equipo) {
      throw new BadRequestException(
        'Debes seleccionar un equipo válido del sistema para el bloque mensual.',
      );
    }

    const descriptor = this.resolveProgramacionMaintenanceDescriptor(rawValue);
    const procedimientos = await this.procedimientoRepo.find({
      where: { is_deleted: false },
    });
    const syncCache = new Map<
      string,
      { plan: PlanMantenimientoEntity; procedimiento: ProcedimientoPlantillaEntity }
    >();

    let mapping = {
      procedimiento_id: null as string | null,
      plan_id: null as string | null,
      es_sincronizable: false,
    };
    if (dto.procedimiento_id) {
      const synced = await this.syncPlanFromProcedimiento(dto.procedimiento_id);
      mapping = {
        procedimiento_id: dto.procedimiento_id,
        plan_id: synced.plan.id,
        es_sincronizable: true,
      };
    } else {
      mapping = await this.resolveProgramacionMensualProcedure(
        descriptor.raw,
        descriptor.frecuencia_horas,
        procedimientos,
        syncCache,
      );
    }

    const fechaProgramada = this.toDateOnlyString(
      dto.fecha_programada ?? current?.fecha_programada,
    );
    if (!fechaProgramada) {
      throw new BadRequestException(
        'Debes indicar la fecha del bloque mensual.',
      );
    }
    const previousPayload = (current?.payload_json ?? {}) as Record<string, unknown>;
    const nextPayload = {
      ...previousPayload,
      ...((dto.payload_json ?? {}) as Record<string, unknown>),
      color_key:
        String(
          ((dto.payload_json ?? {}) as Record<string, unknown>).color_key ??
            descriptor.tipo_mantenimiento,
        ).toUpperCase(),
    } as Record<string, unknown>;
    const horometroUltimo =
      nextPayload.horometro_ultimo != null
        ? this.toNumeric(nextPayload.horometro_ultimo, 0)
        : current?.payload_json?.horometro_ultimo != null
          ? this.toNumeric(current.payload_json.horometro_ultimo, 0)
          : null;
    const proximaHoras =
      descriptor.frecuencia_horas != null && horometroUltimo != null
        ? Number((horometroUltimo + descriptor.frecuencia_horas).toFixed(2))
        : null;
    if (proximaHoras != null) {
      nextPayload.horometro_programado = proximaHoras;
    }
    nextPayload.horas_programadas = descriptor.frecuencia_horas ?? null;

    return {
      equipo,
      fechaProgramada,
      descriptor,
      mapping,
      nextPayload,
      observacion: dto.observacion ?? current?.observacion ?? null,
      proximaHoras,
    };
  }

  async createProgramacionMensualDetalle(
    programacionMensualId: string,
    dto: CreateProgramacionMensualDetalleDto,
  ) {
    const header = await this.findOneOrFail(this.programacionMensualRepo, {
      id: programacionMensualId,
      is_deleted: false,
    });
    const prepared = await this.prepareProgramacionMensualDetailInput(dto);
    let programacionId: string | null = null;
    if (prepared.mapping.es_sincronizable && prepared.mapping.plan_id) {
      const programacion = await this.upsertCalendarProgramacionFromMonthlyDetail({
        equipo_id: prepared.equipo.id,
        plan_id: prepared.mapping.plan_id,
        fecha_programada: prepared.fechaProgramada,
        documento_origen: header.documento_origen || 'MENSUAL_MANUAL',
        valor_crudo: prepared.descriptor.raw,
        frecuencia_horas: prepared.descriptor.frecuencia_horas,
        ultima_ejecucion_horas:
          prepared.nextPayload.horometro_ultimo != null
            ? this.toNumeric(prepared.nextPayload.horometro_ultimo, 0)
            : null,
        proxima_horas: prepared.proximaHoras,
        payload_json: {
          programacion_mensual_codigo: header.codigo,
          tipo_mantenimiento: prepared.descriptor.tipo_mantenimiento,
          valor_normalizado: prepared.descriptor.normalized,
          ...prepared.nextPayload,
        },
      });
      programacionId = programacion.id;
    }
    await this.programacionMensualDetRepo.save(
      this.programacionMensualDetRepo.create({
        programacion_mensual_id: header.id,
        programacion_id: programacionId,
        equipo_id: prepared.equipo.id,
        equipo_codigo: prepared.equipo.codigo,
        equipo_nombre: prepared.equipo.nombre,
        fecha_programada: prepared.fechaProgramada,
        dia_mes: Number(prepared.fechaProgramada.slice(-2)),
        valor_crudo: prepared.descriptor.raw,
        valor_normalizado: prepared.descriptor.normalized,
        tipo_mantenimiento: prepared.descriptor.tipo_mantenimiento,
        frecuencia_horas: prepared.descriptor.frecuencia_horas,
        procedimiento_id: prepared.mapping.procedimiento_id,
        plan_id: prepared.mapping.plan_id,
        es_sincronizable:
          Boolean(prepared.mapping.es_sincronizable) &&
          Boolean(prepared.equipo.id),
        observacion: prepared.observacion,
        payload_json: prepared.nextPayload,
      }),
    );
    return this.wrap(
      await this.buildProgramacionMensualPayload(header),
      'Detalle mensual creado',
    );
  }

  async updateProgramacionMensualDetalle(
    detailId: string,
    dto: UpdateProgramacionMensualDetalleDto,
  ) {
    const detail = await this.findOneOrFail(this.programacionMensualDetRepo, {
      id: detailId,
      is_deleted: false,
    });
    const header = await this.findOneOrFail(this.programacionMensualRepo, {
      id: detail.programacion_mensual_id,
      is_deleted: false,
    });
    const prepared = await this.prepareProgramacionMensualDetailInput(dto, detail);
    let programacionId = detail.programacion_id ?? null;
    if (prepared.mapping.es_sincronizable && prepared.mapping.plan_id) {
      const programacion = await this.upsertCalendarProgramacionFromMonthlyDetail({
        equipo_id: prepared.equipo.id,
        plan_id: prepared.mapping.plan_id,
        fecha_programada: prepared.fechaProgramada,
        documento_origen: header.documento_origen || 'MENSUAL_EDIT',
        valor_crudo: prepared.descriptor.raw,
        frecuencia_horas: prepared.descriptor.frecuencia_horas,
        ultima_ejecucion_horas:
          prepared.nextPayload.horometro_ultimo != null
            ? this.toNumeric(prepared.nextPayload.horometro_ultimo, 0)
            : null,
        proxima_horas: prepared.proximaHoras,
        payload_json: {
          programacion_mensual_codigo: header.codigo,
          tipo_mantenimiento: prepared.descriptor.tipo_mantenimiento,
          valor_normalizado: prepared.descriptor.normalized,
          ...prepared.nextPayload,
        },
      });
      programacionId = programacion.id;
    } else if (detail.programacion_id) {
      await this.disableProgramacionLink(detail.programacion_id);
      programacionId = null;
    }

    Object.assign(detail, {
      programacion_id: programacionId,
      equipo_id: prepared.equipo.id,
      equipo_codigo: prepared.equipo.codigo,
      equipo_nombre: prepared.equipo.nombre,
      fecha_programada: prepared.fechaProgramada,
      dia_mes: Number(prepared.fechaProgramada.slice(-2)),
      valor_crudo: prepared.descriptor.raw,
      valor_normalizado: prepared.descriptor.normalized,
      tipo_mantenimiento: prepared.descriptor.tipo_mantenimiento,
      frecuencia_horas: prepared.descriptor.frecuencia_horas,
      procedimiento_id: prepared.mapping.procedimiento_id,
      plan_id: prepared.mapping.plan_id,
      es_sincronizable:
        Boolean(prepared.mapping.es_sincronizable) &&
        Boolean(prepared.equipo.id),
      observacion: prepared.observacion,
      payload_json: prepared.nextPayload,
    });
    await this.programacionMensualDetRepo.save(detail);
    return this.wrap(
      await this.buildProgramacionMensualPayload(header),
      'Detalle mensual actualizado',
    );
  }

  async updateProgramacionMensualConfig(
    id: string,
    dto: UpdateProgramacionMensualConfigDto,
  ) {
    const header = await this.findOneOrFail(this.programacionMensualRepo, {
      id,
      is_deleted: false,
    });
    const currentPayload = (header.payload_json ?? {}) as Record<string, unknown>;
    header.payload_json = {
      ...currentPayload,
      ...((dto.payload_json ?? {}) as Record<string, unknown>),
      color_palette: {
        ...this.resolveProgramacionMensualColorPalette(currentPayload),
        ...((dto.color_palette ?? {}) as Record<string, string>),
      },
    };
    await this.programacionMensualRepo.save(header);
    return this.wrap(
      await this.buildProgramacionMensualPayload(header),
      'Configuración del mensual actualizada',
    );
  }

  async listProgramacionesMensuales(
    query?: ProgramacionMensualQueryDto,
    sucursalId?: string | null,
  ) {
    const scope = await this.buildSucursalScopeContext(sucursalId);
    const rows = await this.programacionMensualRepo.find({
      where: { is_deleted: false },
      order: { fecha_inicio: 'DESC', created_at: 'DESC' },
    });
    const payload = (
      await Promise.all(
        rows.map(async (row) =>
          this.scopeProgramacionMensualPayload(
            await this.buildProgramacionMensualPayload(row, query),
            scope,
          ),
        ),
      )
    ).filter((item): item is NonNullable<typeof item> => Boolean(item));
    return this.wrap(payload, 'Programaciones mensuales listadas');
  }

  async getProgramacionMensual(
    id: string,
    query?: ProgramacionMensualQueryDto,
    sucursalId?: string | null,
  ) {
    const row = await this.findOneOrFail(this.programacionMensualRepo, {
      id,
      is_deleted: false,
    });
    const scope = await this.buildSucursalScopeContext(sucursalId);
    const payload = this.scopeProgramacionMensualPayload(
      await this.buildProgramacionMensualPayload(row, query),
      scope,
    );
    if (!payload) {
      throw new NotFoundException('Programación mensual no encontrada');
    }
    return this.wrap(
      payload,
      'Programación mensual obtenida',
    );
  }

  async importProgramacionMensualWorkbook(
    file?: {
      buffer?: Buffer;
      originalname?: string;
    } | null,
    options?: {
      requested_by?: string | null;
      requested_by_email?: string | null;
      requested_user_id?: string | null;
      sucursal_id?: string | null;
    },
    scopedSucursalId?: string | null,
  ) {
    if (!file?.buffer) {
      throw new BadRequestException(
        'Debes adjuntar el archivo Excel de programación mensual.',
      );
    }

    const parsed = await this.parseProgramacionMensualWorkbook(
      file.buffer,
      String(file.originalname || 'programacion_mensual.xlsx'),
    );

    const requestedBy = this.firstNonEmptyString(options?.requested_by);
    const requestedByEmail = this.normalizeEmail(options?.requested_by_email);
    const requestedUserId = this.firstNonEmptyString(options?.requested_user_id);
    const sucursal = await this.resolveSucursalForWrite(
      options?.sucursal_id,
      scopedSucursalId,
    );
    const sucursalLabel = this.buildSucursalLabel(sucursal);

    const savedId = await this.dataSource.transaction(async (manager) => {
      const headerRepo = manager.getRepository(ProgramacionMensualEntity);
      const detailRepo = manager.getRepository(ProgramacionMensualDetalleEntity);

      const header = await headerRepo.save(
        headerRepo.create({
          ...parsed.header,
          sucursal_id: sucursal?.id ?? parsed.header.sucursal_id ?? null,
          locacion: parsed.header.locacion ?? sucursalLabel ?? null,
          created_by: requestedBy,
          updated_by: requestedBy,
          payload_json: {
            ...((parsed.header.payload_json ?? {}) as Record<string, unknown>),
            actor_username: requestedBy,
            actor_email: requestedByEmail,
            actor_user_id: requestedUserId,
            requested_by: requestedBy,
            requested_by_email: requestedByEmail,
            sucursal_id: sucursal?.id ?? parsed.header.sucursal_id ?? null,
            sucursal_codigo: sucursal?.codigo ?? null,
            sucursal_nombre: sucursal?.nombre ?? null,
          },
        }),
      );

      for (const detail of parsed.detalles) {
        let programacionId: string | null = null;
        if (detail.es_sincronizable && detail.equipo_id && detail.plan_id) {
          const programacion = await this.upsertCalendarProgramacionFromMonthlyDetail({
            equipo_id: detail.equipo_id,
            plan_id: detail.plan_id,
            fecha_programada: detail.fecha_programada,
            documento_origen: parsed.header.documento_origen,
            valor_crudo: detail.valor_crudo,
            frecuencia_horas: detail.frecuencia_horas,
            ultima_ejecucion_horas:
              detail.payload_json?.horometro_ultimo != null
                ? this.toNumeric(detail.payload_json.horometro_ultimo, 0)
                : null,
            proxima_horas:
              detail.payload_json?.horometro_programado != null
                ? this.toNumeric(detail.payload_json.horometro_programado, 0)
                : null,
            payload_json: {
              programacion_mensual_codigo: parsed.header.codigo,
              tipo_mantenimiento: detail.tipo_mantenimiento,
              valor_normalizado: detail.valor_normalizado,
              horometro_ultimo:
                detail.payload_json?.horometro_ultimo ?? null,
              horometro_actual:
                detail.payload_json?.horometro_actual ?? null,
              horometro_programado:
                detail.payload_json?.horometro_programado ?? null,
              actor_username: requestedBy,
              actor_email: requestedByEmail,
              actor_user_id: requestedUserId,
            },
          });
          programacionId = programacion.id;
        }

        await detailRepo.save(
          detailRepo.create({
            programacion_mensual_id: header.id,
            programacion_id: programacionId,
            ...detail,
            payload_json: {
              ...(detail.payload_json ?? {}),
              actor_username: requestedBy,
              actor_email: requestedByEmail,
              actor_user_id: requestedUserId,
            },
          }),
        );
      }

      return header.id;
    });

    const saved = await this.findOneOrFail(this.programacionMensualRepo, {
      id: savedId,
      is_deleted: false,
    });
    const payload = await this.buildProgramacionMensualPayload(saved);
    await this.registerProcessEvent({
      tipo_proceso: 'PROGRAMACION_MENSUAL',
      accion: 'IMPORTED',
      referencia_tabla: 'tb_programacion_mensual',
      referencia_id: saved.id,
      referencia_codigo: saved.codigo,
      title: 'Programación mensual importada',
      body: `${saved.codigo} · ${saved.nombre_archivo || saved.documento_origen || 'Excel mensual'}`,
      payload_kpi: {
        detalles: payload.total_detalles,
        periodos: payload.periodos.length,
      },
    });

    return this.wrap(
      {
        ...payload,
        warnings: parsed.warnings,
      },
      'Programación mensual importada desde Excel',
    );
  }

  private async parseCronogramaSemanalWorkbook(
    buffer: Buffer,
    fileName: string,
    sucursal?: InventorySucursalEntity | null,
  ): Promise<ParsedCronogramaSemanalWorkbook> {
    const workbook = XLSX.read(buffer, { type: 'buffer', cellDates: true });
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName || ''];
    if (!sheet) {
      throw new BadRequestException(
        'El archivo semanal no contiene una hoja válida para procesar.',
      );
    }

    const warnings: string[] = [];
    const range = XLSX.utils.decode_range(sheet['!ref'] || 'A1:A1');
    const dateRange =
      [2, 3, 4, 5]
        .map((rowNumber) =>
          this.parseCronogramaDateRangeLabel(
            this.getWorkbookCellValue(sheet, rowNumber, 1),
          ),
        )
        .find(Boolean) ?? null;

    if (!dateRange) {
      throw new BadRequestException(
        'No se pudo identificar el rango semanal del cronograma.',
      );
    }

    const detalles: CreateCronogramaSemanalDto['detalles'] = [];
    const dailyHours: Record<string, number> = {};
    const start = new Date(`${dateRange.fecha_inicio}T00:00:00Z`);

    for (let rowNumber = 7; rowNumber <= range.e.r + 1; rowNumber += 1) {
      const slot = this.parseWorkbookTimeRange(
        this.getWorkbookCellValue(sheet, rowNumber, 1),
      );
      if (!slot) continue;

      for (let column = 2; column <= 8; column += 1) {
        const activity = this.getWorkbookCellText(sheet, rowNumber, column);
        if (!activity) continue;
        const dayLabel = this.normalizeProgramacionWorkbookValue(
          this.getWorkbookCellValue(sheet, 5, column),
        );
        const activityDate = new Date(start);
        activityDate.setUTCDate(start.getUTCDate() + (column - 2));
        const tipoProceso = this.resolveCronogramaTipoProceso(activity);
        const equipoCodigo = this.extractEquipoCodigoFromText(activity);
        const fechaActividad = activityDate.toISOString().slice(0, 10);
        dailyHours[fechaActividad] = Number(
          ((dailyHours[fechaActividad] ?? 0) + slot.duracion_horas).toFixed(2),
        );

        detalles.push({
          dia_semana: dayLabel || `Día ${column - 1}`,
          fecha_actividad: fechaActividad,
          hora_inicio: slot.hora_inicio,
          hora_fin: slot.hora_fin,
          tipo_proceso: tipoProceso,
          actividad: activity,
          responsable_area:
            tipoProceso === 'SSA'
              ? 'SSA'
              : tipoProceso === 'MPG'
                ? 'MANTENIMIENTO'
                : undefined,
          equipo_codigo: equipoCodigo ?? undefined,
          observacion: undefined,
          orden: detalles.length + 1,
        });
      }
    }

    if (!detalles.length) {
      throw new BadRequestException(
        'El archivo semanal no contiene actividades válidas para importar.',
      );
    }

    return {
      dto: {
        codigo: await this.generateNextCronogramaSemanalCode(),
        fecha_inicio: dateRange.fecha_inicio,
        fecha_fin: dateRange.fecha_fin,
        sucursal_id: sucursal?.id ?? undefined,
        locacion: this.buildSucursalLabel(sucursal) ?? 'TPTA',
        referencia_orden: undefined,
        documento_origen: fileName,
        resumen: `Importación semanal desde ${fileName}`,
        payload_json: {
          hoja_origen: sheetName,
          rango_fuente: dateRange.label,
          daily_hours: dailyHours,
          sucursal_id: sucursal?.id ?? null,
          sucursal_codigo: sucursal?.codigo ?? null,
          sucursal_nombre: sucursal?.nombre ?? null,
        },
        detalles,
      },
      warnings,
    };
  }

  async importCronogramaSemanalWorkbook(
    file?: {
      buffer?: Buffer;
      originalname?: string;
    } | null,
    options?: {
      requested_by?: string | null;
      requested_by_email?: string | null;
      requested_user_id?: string | null;
      sucursal_id?: string | null;
    },
    scopedSucursalId?: string | null,
  ) {
    if (!file?.buffer) {
      throw new BadRequestException(
        'Debes adjuntar el archivo Excel del cronograma semanal.',
      );
    }

    const sucursal = await this.resolveSucursalForWrite(
      options?.sucursal_id,
      scopedSucursalId,
    );
    const parsed = await this.parseCronogramaSemanalWorkbook(
      file.buffer,
      String(file.originalname || 'cronograma_semanal.xlsx'),
      sucursal,
    );
    const created = await this.createCronogramaSemanal({
      ...parsed.dto,
      payload_json: {
        ...((parsed.dto.payload_json ?? {}) as Record<string, unknown>),
        actor_username: this.firstNonEmptyString(options?.requested_by),
        actor_email: this.normalizeEmail(options?.requested_by_email),
        actor_user_id: this.firstNonEmptyString(options?.requested_user_id),
      },
    }, scopedSucursalId);
    return this.wrap(
      {
        cronograma: created.data,
        warnings: parsed.warnings,
      },
      'Cronograma semanal importado desde Excel',
    );
  }

  async listCronogramasSemanales(sucursalId?: string | null) {
    const scope = await this.buildSucursalScopeContext(sucursalId);
    const rows = await this.cronogramaSemanalRepo.find({
      where: { is_deleted: false },
      order: { fecha_inicio: 'DESC', created_at: 'DESC' },
    });
    return this.wrap(
      (
        await Promise.all(
          rows.map(async (row) =>
            this.scopeCronogramaSemanalPayload(
              await this.buildCronogramaSemanalPayload(row),
              scope,
            ),
          ),
        )
      ).filter((item): item is NonNullable<typeof item> => Boolean(item)),
      'Cronogramas semanales listados',
    );
  }

  async getCronogramaSemanal(id: string, sucursalId?: string | null) {
    const row = await this.findOneOrFail(this.cronogramaSemanalRepo, {
      id,
      is_deleted: false,
    });
    const scope = await this.buildSucursalScopeContext(sucursalId);
    const payload = this.scopeCronogramaSemanalPayload(
      await this.buildCronogramaSemanalPayload(row),
      scope,
    );
    if (!payload) {
      throw new NotFoundException('Cronograma semanal no encontrado');
    }
    return this.wrap(
      payload,
      'Cronograma semanal obtenido',
    );
  }

  async createCronogramaSemanal(
    dto: CreateCronogramaSemanalDto,
    scopedSucursalId?: string | null,
  ) {
    const sucursal = await this.resolveSucursalForWrite(
      dto.sucursal_id,
      scopedSucursalId,
    );
    const sucursalLabel = this.buildSucursalLabel(sucursal);
    const savedId = await this.dataSource.transaction(async (manager) => {
      const cronogramaRepo = manager.getRepository(CronogramaSemanalEntity);
      const detalleRepo = manager.getRepository(CronogramaSemanalDetalleEntity);
      const row = cronogramaRepo.create();
      const payloadJson = ((dto.payload_json ?? {}) ||
        {}) as Record<string, unknown>;
      Object.assign(row, {
        codigo: dto.codigo,
        fecha_inicio: dto.fecha_inicio.slice(0, 10),
        fecha_fin: dto.fecha_fin.slice(0, 10),
        sucursal_id: sucursal?.id ?? null,
        locacion: dto.locacion ?? sucursalLabel ?? null,
        referencia_orden: dto.referencia_orden ?? null,
        documento_origen: dto.documento_origen ?? null,
        resumen: dto.resumen ?? null,
        payload_json: {
          ...payloadJson,
          sucursal_id: sucursal?.id ?? null,
          sucursal_codigo: sucursal?.codigo ?? null,
          sucursal_nombre: sucursal?.nombre ?? null,
        },
        created_by: this.firstNonEmptyString(
          payloadJson.actor_username,
          payloadJson.created_by,
          payloadJson.updated_by,
        ),
        updated_by: this.firstNonEmptyString(
          payloadJson.actor_username,
          payloadJson.updated_by,
          payloadJson.created_by,
        ),
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

  async updateCronogramaSemanal(
    id: string,
    dto: UpdateCronogramaSemanalDto,
    scopedSucursalId?: string | null,
  ) {
    await this.findOneOrFail(this.cronogramaSemanalRepo, { id, is_deleted: false });

    await this.dataSource.transaction(async (manager) => {
      const cronogramaRepo = manager.getRepository(CronogramaSemanalEntity);
      const detalleRepo = manager.getRepository(CronogramaSemanalDetalleEntity);
      const row = await cronogramaRepo.findOne({ where: { id, is_deleted: false } });
      if (!row) throw new NotFoundException('Cronograma semanal no encontrado');
      const sucursal = await this.resolveSucursalForWrite(
        dto.sucursal_id ?? row.sucursal_id,
        scopedSucursalId,
      );
      const sucursalLabel = this.buildSucursalLabel(sucursal);

      Object.assign(row, {
        codigo: dto.codigo ?? row.codigo,
        fecha_inicio: dto.fecha_inicio ? this.toDateOnlyString(dto.fecha_inicio) : row.fecha_inicio,
        fecha_fin: dto.fecha_fin ? this.toDateOnlyString(dto.fecha_fin) : row.fecha_fin,
        sucursal_id: sucursal?.id ?? row.sucursal_id ?? null,
        locacion: dto.locacion ?? sucursalLabel ?? row.locacion ?? null,
        referencia_orden: dto.referencia_orden ?? row.referencia_orden ?? null,
        documento_origen: dto.documento_origen ?? row.documento_origen ?? null,
        resumen: dto.resumen ?? row.resumen ?? null,
        payload_json: {
          ...((row.payload_json ?? {}) as Record<string, unknown>),
          ...((dto.payload_json ?? {}) as Record<string, unknown>),
          sucursal_id: sucursal?.id ?? row.sucursal_id ?? null,
          sucursal_codigo: sucursal?.codigo ?? null,
          sucursal_nombre: sucursal?.nombre ?? null,
        },
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

  async listReportesOperacionDiaria(sucursalId?: string | null) {
    const scope = await this.buildSucursalScopeContext(sucursalId);
    const rows = await this.reporteDiarioRepo.find({
      where: { is_deleted: false },
      order: { fecha_reporte: 'DESC', created_at: 'DESC' },
    });
    return this.wrap(
      (
        await Promise.all(
          rows.map(async (row) =>
            this.scopeReporteDiarioPayload(
              await this.buildReporteDiarioPayload(row),
              scope,
            ),
          ),
        )
      ).filter((item): item is NonNullable<typeof item> => Boolean(item)),
      'Reportes de operación diaria listados',
    );
  }

  async getReporteOperacionDiaria(id: string, sucursalId?: string | null) {
    const row = await this.findOneOrFail(this.reporteDiarioRepo, {
      id,
      is_deleted: false,
    });
    const scope = await this.buildSucursalScopeContext(sucursalId);
    const payload = this.scopeReporteDiarioPayload(
      await this.buildReporteDiarioPayload(row),
      scope,
    );
    if (!payload) {
      throw new NotFoundException('Reporte de operación diaria no encontrado');
    }
    return this.wrap(
      payload,
      'Reporte de operación diaria obtenido',
    );
  }

  async createReporteOperacionDiaria(
    dto: CreateReporteOperacionDiariaDto,
    scopedSucursalId?: string | null,
  ) {
    const sucursal = await this.resolveSucursalForWrite(
      dto.sucursal_id,
      scopedSucursalId,
    );
    const sucursalLabel = this.buildSucursalLabel(sucursal);
    const savedId = await this.dataSource.transaction(async (manager) => {
      const reporteRepo = manager.getRepository(ReporteOperacionDiariaEntity);
      const unidadRepo = manager.getRepository(ReporteOperacionDiariaUnidadEntity);
      const combustibleRepo = manager.getRepository(ReporteCombustibleEntity);
      const componenteRepo = manager.getRepository(ControlComponenteEntity);

      const payloadJson = ((dto.payload_json ?? {}) ||
        {}) as Record<string, unknown>;
      const row = reporteRepo.create();
      Object.assign(row, {
        codigo: dto.codigo,
        fecha_reporte: dto.fecha_reporte.slice(0, 10),
        sucursal_id: sucursal?.id ?? null,
        locacion: dto.locacion ?? sucursalLabel ?? null,
        turno: dto.turno ?? null,
        documento_origen: dto.documento_origen ?? null,
        resumen: dto.resumen ?? null,
        payload_json: {
          ...payloadJson,
          sucursal_id: sucursal?.id ?? null,
          sucursal_codigo: sucursal?.codigo ?? null,
          sucursal_nombre: sucursal?.nombre ?? null,
        },
        created_by: this.firstNonEmptyString(
          payloadJson.actor_username,
          payloadJson.created_by,
          payloadJson.updated_by,
        ),
        updated_by: this.firstNonEmptyString(
          payloadJson.actor_username,
          payloadJson.updated_by,
          payloadJson.created_by,
        ),
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

  async updateReporteOperacionDiaria(
    id: string,
    dto: UpdateReporteOperacionDiariaDto,
    scopedSucursalId?: string | null,
  ) {
    await this.findOneOrFail(this.reporteDiarioRepo, { id, is_deleted: false });

    await this.dataSource.transaction(async (manager) => {
      const reporteRepo = manager.getRepository(ReporteOperacionDiariaEntity);
      const unidadRepo = manager.getRepository(ReporteOperacionDiariaUnidadEntity);
      const combustibleRepo = manager.getRepository(ReporteCombustibleEntity);
      const componenteRepo = manager.getRepository(ControlComponenteEntity);
      const row = await reporteRepo.findOne({ where: { id, is_deleted: false } });
      if (!row) throw new NotFoundException('Reporte de operación diaria no encontrado');
      const sucursal = await this.resolveSucursalForWrite(
        dto.sucursal_id ?? row.sucursal_id,
        scopedSucursalId,
      );
      const sucursalLabel = this.buildSucursalLabel(sucursal);

      Object.assign(row, {
        codigo: dto.codigo ?? row.codigo,
        fecha_reporte: dto.fecha_reporte
          ? this.toDateOnlyString(dto.fecha_reporte)
          : row.fecha_reporte,
        sucursal_id: sucursal?.id ?? row.sucursal_id ?? null,
        locacion: dto.locacion ?? sucursalLabel ?? row.locacion ?? null,
        turno: dto.turno ?? row.turno ?? null,
        documento_origen: dto.documento_origen ?? row.documento_origen ?? null,
        resumen: dto.resumen ?? row.resumen ?? null,
        payload_json: {
          ...((row.payload_json ?? {}) as Record<string, unknown>),
          ...((dto.payload_json ?? {}) as Record<string, unknown>),
          sucursal_id: sucursal?.id ?? row.sucursal_id ?? null,
          sucursal_codigo: sucursal?.codigo ?? null,
          sucursal_nombre: sucursal?.nombre ?? null,
        },
        updated_by: this.firstNonEmptyString(
          (dto.payload_json ?? {})?.['actor_username'],
          (dto.payload_json ?? {})?.['updated_by'],
          (dto.payload_json ?? {})?.['created_by'],
          row.updated_by,
          row.created_by,
        ),
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

  private buildIntelligencePeriodRange(query?: IntelligencePeriodQueryDto) {
    const year = Number(query?.year ?? 0);
    const month = Number(query?.month ?? 0);
    if (!year || !month || month < 1 || month > 12) return null;
    const start = new Date(Date.UTC(year, month - 1, 1));
    const end = new Date(Date.UTC(year, month, 0, 23, 59, 59, 999));
    return { year, month, start, end };
  }

  private valueMatchesIntelligencePeriod(
    value: unknown,
    range: ReturnType<KpiMaintenanceService['buildIntelligencePeriodRange']>,
  ) {
    if (!range) return true;
    const raw = this.safeDateOnlyString(value) ?? String(value || '').trim();
    if (!raw) return false;
    const parsed = new Date(
      /^\d{4}-\d{2}-\d{2}$/.test(raw) ? `${raw}T00:00:00Z` : raw,
    );
    if (Number.isNaN(parsed.getTime())) return false;
    return parsed >= range.start && parsed <= range.end;
  }

  private rangeOverlapsIntelligencePeriod(
    fromValue: unknown,
    toValue: unknown,
    range: ReturnType<KpiMaintenanceService['buildIntelligencePeriodRange']>,
  ) {
    if (!range) return true;
    const fromRaw =
      this.safeDateOnlyString(fromValue) ?? String(fromValue || '').trim();
    const toRaw =
      this.safeDateOnlyString(toValue) ??
      this.safeDateOnlyString(fromValue) ??
      String(toValue || '').trim();
    if (!fromRaw && !toRaw) return false;
    const from = new Date(
      /^\d{4}-\d{2}-\d{2}$/.test(fromRaw) ? `${fromRaw}T00:00:00Z` : fromRaw,
    );
    const to = new Date(
      /^\d{4}-\d{2}-\d{2}$/.test(toRaw) ? `${toRaw}T23:59:59.999Z` : toRaw,
    );
    if (Number.isNaN(from.getTime()) || Number.isNaN(to.getTime())) return false;
    return from <= range.end && to >= range.start;
  }

  async getIntelligenceSummary(
    query?: IntelligencePeriodQueryDto,
    sucursalId?: string | null,
  ) {
    const periodRange = this.buildIntelligencePeriodRange(query);
    const [
      procedimientosRows,
      analisisRows,
      cronogramas,
      reportes,
      eventos,
      overdueProgramaciones,
      pendingWorkOrders,
      componentesRecientes,
    ] = await Promise.all([
      this.procedimientoRepo.find({ where: { is_deleted: false } }),
      this.analisisLubricanteRepo.find({
        where: { is_deleted: false },
        order: { fecha_reporte: 'DESC', created_at: 'DESC' },
      }),
      this.cronogramaSemanalRepo.find({
        where: { is_deleted: false },
        order: { fecha_inicio: 'DESC', created_at: 'DESC' },
      }),
      this.reporteDiarioRepo.find({
        where: { is_deleted: false },
        order: { fecha_reporte: 'DESC', created_at: 'DESC' },
      }),
      this.eventoProcesoRepo.find({
        where: { is_deleted: false },
        order: { fecha_evento: 'DESC', created_at: 'DESC' },
      }),
      this.programacionRepo.find({ where: { is_deleted: false, activo: true } }),
      this.woRepo.find({
        where: { is_deleted: false, status_workflow: In(['PLANNED', 'IN_PROGRESS']) },
        order: { scheduled_start: 'DESC' },
      }),
      this.controlComponenteRepo.find({
        where: { is_deleted: false },
        order: { updated_at: 'DESC', created_at: 'DESC' },
      }),
    ]);
    const scope = await this.buildSucursalScopeContext(sucursalId);
    const procedimientos = procedimientosRows.filter((row) =>
      this.isProcedimientoVisibleForScope(row, scope),
    );

    const programaciones = await Promise.all(
      overdueProgramaciones.map((row) =>
        this.recalculateProgramacionFields(row, { persist: false }),
      ),
    );

    const scopedAnalyses = analisisRows.filter((row) =>
      this.matchesScopedEquipment(row.equipo_id, row.equipo_codigo, scope),
    );
    const scopedCronogramas = !scope
      ? cronogramas
      : cronogramas.filter((row) =>
          this.matchesScopedLocation(row.locacion, scope),
        );
    const scopedReportes = !scope
      ? reportes
      : reportes.filter((row) =>
          this.matchesScopedLocation(row.locacion, scope),
        );
    const scopedEventos = !scope
      ? eventos
      : eventos.filter((row) =>
          this.matchesScopedEquipment(row.equipo_id, null, scope),
        );
    const scopedProgramaciones = !scope
      ? programaciones
      : programaciones.filter((row: any) =>
          this.matchesScopedEquipment(row.equipo_id, row.equipo_codigo, scope),
        );
    const scopedPendingWorkOrders = await this.filterWorkOrdersByScope(
      pendingWorkOrders,
      scope,
    );
    const scopedComponents = !scope
      ? componentesRecientes
      : componentesRecientes.filter((row) =>
          this.matchesScopedEquipment(row.equipo_id, row.equipo_codigo, scope),
        );

    const filteredAnalyses = scopedAnalyses.filter((row) =>
      this.valueMatchesIntelligencePeriod(
        row.fecha_reporte || row.fecha_muestra || row.created_at,
        periodRange,
      ),
    );
    const filteredCronogramas = scopedCronogramas.filter((row) =>
      this.rangeOverlapsIntelligencePeriod(
        row.fecha_inicio || row.created_at,
        row.fecha_fin || row.fecha_inicio || row.created_at,
        periodRange,
      ),
    );
    const filteredReportes = scopedReportes.filter((row) =>
      this.valueMatchesIntelligencePeriod(
        row.fecha_reporte || row.created_at,
        periodRange,
      ),
    );
    const filteredEventos = scopedEventos.filter((row) =>
      this.valueMatchesIntelligencePeriod(
        row.fecha_evento || row.created_at,
        periodRange,
      ),
    );
    const filteredProgramaciones = scopedProgramaciones.filter((row: any) =>
      this.valueMatchesIntelligencePeriod(
        row.proxima_fecha || row.updated_at || row.created_at,
        periodRange,
      ),
    );
    const vencidas = filteredProgramaciones.filter(
      (row: any) =>
        String(row.estado_programacion || '').toUpperCase() === 'VENCIDA',
    );

    const filteredPendingWorkOrders = scopedPendingWorkOrders.filter((row) =>
      this.valueMatchesIntelligencePeriod(
        row.scheduled_start || row.started_at || row.closed_at,
        periodRange,
      ),
    );

    const reportIds = new Set(
      filteredReportes.map((row) => String(row.id || '')).filter(Boolean),
    );
    const filteredComponents = scopedComponents.filter((row) =>
      row.reporte_id
        ? reportIds.has(String(row.reporte_id))
        : this.valueMatchesIntelligencePeriod(
            row.updated_at || row.created_at,
            periodRange,
          ),
    );

    const breakdown = filteredEventos.reduce<Record<string, number>>((acc, event) => {
      const key = String(event.tipo_proceso || 'SIN_TIPO');
      acc[key] = (acc[key] ?? 0) + 1;
      return acc;
    }, {});
    const lubricantesRegistrados = new Set(
      filteredAnalyses
        .map(
          (item) =>
            this.resolveLubricantIdentity(item).lubricante_lookup_key || null,
        )
        .filter(Boolean),
    ).size;

    return this.wrap(
      {
        generated_at: new Date().toISOString(),
        filters: periodRange
          ? {
              year: periodRange.year,
              month: periodRange.month,
              from: periodRange.start.toISOString().slice(0, 10),
              to: periodRange.end.toISOString().slice(0, 10),
            }
          : null,
        kpis: {
          procedimientos: procedimientos.length,
          analisis_lubricante: filteredAnalyses.length,
          lubricantes_registrados: lubricantesRegistrados,
          cronogramas_semanales: filteredCronogramas.length,
          reportes_diarios: filteredReportes.length,
          eventos_proceso: filteredEventos.length,
          programaciones_vencidas: vencidas.length,
          work_orders_pendientes: filteredPendingWorkOrders.length,
          componentes_monitoreados: filteredComponents.length,
        },
        process_breakdown: Object.entries(breakdown).map(([tipo_proceso, total]) => ({
          tipo_proceso,
          total,
        })),
        recent_events: filteredEventos.slice(0, 8),
        recent_analyses: filteredAnalyses.slice(0, 5),
        recent_weekly_schedules: filteredCronogramas.slice(0, 5),
        recent_daily_reports: filteredReportes.slice(0, 5),
        component_highlights: filteredComponents.slice(0, 5),
      },
      'Resumen de inteligencia operativa generado',
    );
  }

  async getAnalisisAceiteKpi(
    query: AnalisisAceiteKpiQueryDto,
    sucursalId?: string | null,
  ) {
    const [scope, oilCatalog] = await Promise.all([
      this.buildSucursalScopeContext(sucursalId),
      this.buildOilProductCatalog(),
    ]);
    const oilCatalogMap = new Map(oilCatalog.map((item) => [item.id, item]));
    const requestedProductId = this.firstNonEmptyString(query?.producto_id);

    if (requestedProductId && !oilCatalogMap.has(requestedProductId)) {
      throw new BadRequestException(
        'El material seleccionado no corresponde a un aceite medido en galones.',
      );
    }

    const selectedProductId =
      requestedProductId ?? this.firstNonEmptyString(oilCatalog[0]?.id);
    const range = this.buildOilUsageDateRange(query);

    if (!selectedProductId) {
      return this.wrap(
        {
          filters: range,
          catalog: oilCatalog,
          selected_product_id: null,
          selected_product: null,
          totals: {
            total_cantidad: 0,
            total_ordenes: 0,
            total_equipos: 0,
            promedio_por_orden: 0,
            promedio_por_equipo: 0,
            total_costo: 0,
          },
          trend: [],
          work_orders: [],
          by_equipment: [],
        },
        'KPI de análisis de aceite generado',
      );
    }

    const consumos = await this.consumoRepo.find({
      where: {
        producto_id: selectedProductId,
        is_deleted: false,
      },
      order: { id: 'DESC' },
    });

    const workOrderIds = [
      ...new Set(
        consumos
          .map((row) => String(row.work_order_id || '').trim())
          .filter(Boolean),
      ),
    ];
    const rawWorkOrders = workOrderIds.length
      ? await this.woRepo.find({
          where: { id: In(workOrderIds), is_deleted: false },
        })
      : [];
    const visibleWorkOrders = await this.filterWorkOrdersByScope(
      rawWorkOrders,
      scope,
    );
    const workOrderMap = new Map(
      visibleWorkOrders.map((row) => [row.id, row]),
    );

    const equipmentIds = [
      ...new Set(
        visibleWorkOrders
          .map((row) => String(row.equipment_id || '').trim())
          .filter(Boolean),
      ),
    ];
    const equipments = equipmentIds.length
      ? await this.equipoRepo.find({
          where: { id: In(equipmentIds), is_deleted: false },
        })
      : [];
    const equipmentMap = new Map(equipments.map((row) => [row.id, row]));

    const { productMap, warehouseMap } = await this.buildInventoryCatalogMaps(
      [selectedProductId],
      consumos.map((row) => row.bodega_id || '').filter(Boolean),
    );

    const groupedByOrder = new Map<
      string,
      {
        work_order_id: string;
        producto_id: string;
        workOrder: WorkOrderEntity;
        referenceDate: Date;
        equipment_id: string | null;
        cantidad: number;
        subtotal: number;
        bodega_ids: Set<string>;
        observaciones: Set<string>;
        movimientos: number;
      }
    >();

    for (const row of consumos) {
      const workOrder = workOrderMap.get(String(row.work_order_id || '').trim());
      if (!workOrder) continue;
      const referenceDate = this.resolveWorkOrderReferenceDate(workOrder);
      if (!referenceDate) continue;
      if (
        referenceDate.getTime() < range.fromDate.getTime() ||
        referenceDate.getTime() > range.toDate.getTime()
      ) {
        continue;
      }

      const key = `${workOrder.id}::${row.producto_id}`;
      const current =
        groupedByOrder.get(key) ??
        {
          work_order_id: workOrder.id,
          producto_id: row.producto_id,
          workOrder,
          referenceDate,
          equipment_id: workOrder.equipment_id ?? null,
          cantidad: 0,
          subtotal: 0,
          bodega_ids: new Set<string>(),
          observaciones: new Set<string>(),
          movimientos: 0,
        };

      current.cantidad += this.toNumeric(row.cantidad, 0);
      current.subtotal += this.toNumeric(row.subtotal, 0);
      current.movimientos += 1;
      if (row.bodega_id) {
        current.bodega_ids.add(String(row.bodega_id).trim());
      }
      if (row.observacion) {
        current.observaciones.add(String(row.observacion).trim());
      }
      groupedByOrder.set(key, current);
    }

    const baseRows = [...groupedByOrder.values()]
      .map((row) => {
        const producto = productMap.get(row.producto_id);
        const equipment = row.equipment_id
          ? equipmentMap.get(row.equipment_id)
          : null;
        const bodegaLabels = [...row.bodega_ids]
          .map((id) => this.buildBodegaLabel(warehouseMap.get(id)) ?? id)
          .filter(Boolean);
        const quantity = Number(row.cantidad.toFixed(4));
        const totalCost = Number(row.subtotal.toFixed(2));

        return {
          work_order_id: row.work_order_id,
          work_order_code: row.workOrder.code,
          work_order_title: row.workOrder.title,
          work_order_status: this.normalizeWorkflowStatus(
            row.workOrder.status_workflow,
          ),
          fecha_referencia: row.referenceDate.toISOString(),
          fecha_referencia_label: this.formatOilUsageDateLabel(row.referenceDate),
          producto_id: row.producto_id,
          producto_codigo: producto?.codigo ?? null,
          producto_nombre: producto?.nombre ?? null,
          producto_label: this.buildProductoLabel(producto) ?? row.producto_id,
          equipment_id: equipment?.id ?? row.equipment_id ?? null,
          equipment_code: equipment?.codigo ?? null,
          equipment_name: equipment?.nombre ?? null,
          equipment_label:
            [equipment?.codigo, equipment?.nombre].filter(Boolean).join(' - ') ||
            'Sin equipo',
          bodegas: bodegaLabels,
          bodega_label: bodegaLabels.join(' | ') || 'Sin bodega',
          movimientos: row.movimientos,
          cantidad: quantity,
          subtotal: totalCost,
          costo_promedio:
            quantity > 0 ? Number((totalCost / quantity).toFixed(4)) : 0,
          observacion: [...row.observaciones].join(' | ') || null,
        };
      })
      .sort((a, b) => {
        const dateDiff =
          new Date(a.fecha_referencia).getTime() -
          new Date(b.fecha_referencia).getTime();
        if (dateDiff !== 0) return dateDiff;
        return String(a.work_order_code || '').localeCompare(
          String(b.work_order_code || ''),
        );
      });

    let previousQuantity: number | null = null;
    const workOrderRows = baseRows.map((row) => {
      const difference =
        previousQuantity == null
          ? null
          : Number((row.cantidad - previousQuantity).toFixed(4));
      previousQuantity = row.cantidad;
      return {
        ...row,
        diferencia_vs_anterior: difference,
        tendencia_cantidad:
          difference == null
            ? 'BASE'
            : difference > 0
              ? 'SUBE'
              : difference < 0
                ? 'BAJA'
                : 'IGUAL',
      };
    });

    const byEquipmentMap = new Map<
      string,
      {
        equipment_id: string | null;
        equipment_label: string;
        equipment_code: string | null;
        equipment_name: string | null;
        total_cantidad: number;
        total_costo: number;
        work_order_ids: Set<string>;
        fechas: Date[];
      }
    >();
    for (const row of workOrderRows) {
      const equipmentKey =
        String(row.equipment_id || '').trim() ||
        `SIN_EQUIPO::${String(row.equipment_label || '').trim()}`;
      const current =
        byEquipmentMap.get(equipmentKey) ??
        {
          equipment_id: row.equipment_id,
          equipment_label: row.equipment_label,
          equipment_code: row.equipment_code,
          equipment_name: row.equipment_name,
          total_cantidad: 0,
          total_costo: 0,
          work_order_ids: new Set<string>(),
          fechas: [],
        };
      current.total_cantidad += this.toNumeric(row.cantidad, 0);
      current.total_costo += this.toNumeric(row.subtotal, 0);
      current.work_order_ids.add(row.work_order_id);
      current.fechas.push(new Date(row.fecha_referencia));
      byEquipmentMap.set(equipmentKey, current);
    }

    const byEquipment = [...byEquipmentMap.values()]
      .map((row) => ({
        equipment_id: row.equipment_id,
        equipment_code: row.equipment_code,
        equipment_name: row.equipment_name,
        equipment_label: row.equipment_label,
        total_cantidad: Number(row.total_cantidad.toFixed(4)),
        total_costo: Number(row.total_costo.toFixed(2)),
        total_ordenes: row.work_order_ids.size,
        primera_fecha:
          row.fechas.length > 0
            ? row.fechas
                .slice()
                .sort((a, b) => a.getTime() - b.getTime())[0]
                ?.toISOString() ?? null
            : null,
        ultima_fecha:
          row.fechas.length > 0
            ? row.fechas
                .slice()
                .sort((a, b) => b.getTime() - a.getTime())[0]
                ?.toISOString() ?? null
            : null,
      }))
      .sort((a, b) => b.total_cantidad - a.total_cantidad);

    const trendMap = new Map<
      string,
      {
        key: string;
        label: string;
        total_cantidad: number;
        work_order_ids: Set<string>;
      }
    >();
    for (const row of workOrderRows) {
      const dateValue = new Date(row.fecha_referencia);
      const key =
        range.periodo === 'ANUAL'
          ? `${dateValue.getFullYear()}-${String(dateValue.getMonth() + 1).padStart(2, '0')}`
          : dateValue.toISOString().slice(0, 10);
      const current =
        trendMap.get(key) ??
        {
          key,
          label: this.formatOilUsageBucketLabel(dateValue, range.periodo),
          total_cantidad: 0,
          work_order_ids: new Set<string>(),
        };
      current.total_cantidad += this.toNumeric(row.cantidad, 0);
      current.work_order_ids.add(row.work_order_id);
      trendMap.set(key, current);
    }

    const trend = [...trendMap.values()]
      .sort((a, b) => a.key.localeCompare(b.key))
      .map((row) => ({
        key: row.key,
        label: row.label,
        cantidad: Number(row.total_cantidad.toFixed(4)),
        total_ordenes: row.work_order_ids.size,
      }));

    const totalCantidad = workOrderRows.reduce(
      (acc, row) => acc + this.toNumeric(row.cantidad, 0),
      0,
    );
    const totalCosto = workOrderRows.reduce(
      (acc, row) => acc + this.toNumeric(row.subtotal, 0),
      0,
    );
    const totalOrdenes = new Set(workOrderRows.map((row) => row.work_order_id))
      .size;
    const totalEquipos = new Set(
      byEquipment.map((row) => row.equipment_id || row.equipment_label),
    ).size;

    const selectedProduct = oilCatalogMap.get(selectedProductId) ?? null;

    return this.wrap(
      {
        filters: range,
        catalog: oilCatalog,
        selected_product_id: selectedProductId,
        selected_product: selectedProduct,
        totals: {
          total_cantidad: Number(totalCantidad.toFixed(4)),
          total_costo: Number(totalCosto.toFixed(2)),
          total_ordenes: totalOrdenes,
          total_equipos: totalEquipos,
          promedio_por_orden:
            totalOrdenes > 0
              ? Number((totalCantidad / totalOrdenes).toFixed(4))
              : 0,
          promedio_por_equipo:
            totalEquipos > 0
              ? Number((totalCantidad / totalEquipos).toFixed(4))
              : 0,
        },
        trend,
        work_orders: workOrderRows,
        by_equipment: byEquipment,
      },
      'KPI de análisis de aceite generado',
    );
  }

  async listAlertas(q: AlertaQueryDto, sucursalId?: string | null) {
    const page = Number.isFinite(Number(q.page)) && Number(q.page) > 0 ? Number(q.page) : 1;
    const limit = Math.min(
      Number.isFinite(Number(q.limit)) && Number(q.limit) > 0 ? Number(q.limit) : 100,
      500,
    );
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
    const scope = await this.buildSucursalScopeContext(sucursalId);
    const enriched = await this.enrichAlertRows(rows);
    const referencedWorkOrderIds = [
      ...new Set(
        enriched.flatMap((row: any) => {
          const ids = Array.isArray(row.work_orders)
            ? row.work_orders.map((item: any) => String(item?.id || '').trim())
            : [];
          if (row.work_order_id) {
            ids.push(String(row.work_order_id || '').trim());
          }
          return ids.filter(Boolean);
        }),
      ),
    ];
    const visibleWorkOrderIds = new Set(
      !scope || !referencedWorkOrderIds.length
        ? referencedWorkOrderIds
        : (
            await this.filterWorkOrdersByScope(
              await this.woRepo.find({
                where: {
                  id: In(referencedWorkOrderIds),
                  is_deleted: false,
                } as any,
              }),
              scope,
            )
          ).map((item) => String(item.id || '').trim()),
    );
    const scopedRows = !scope
      ? enriched
      : enriched
          .map((row: any) => {
            const payload = (row.payload_json ?? {}) as Record<string, unknown>;
            const inventoryItems = this.getInventoryAlertItems(payload).filter((item) =>
              scope.warehouseIds.has(String(item.bodega_id || '').trim()),
            );
            const linkedWorkOrders = Array.isArray(row.work_orders)
              ? row.work_orders.filter((item: any) =>
                  visibleWorkOrderIds.has(String(item?.id || '').trim()),
                )
              : [];
            const workOrderVisible =
              linkedWorkOrders.length > 0 ||
              (row.work_order_id &&
                visibleWorkOrderIds.has(String(row.work_order_id || '').trim()));
            const equipmentVisible = this.matchesScopedEquipment(
              row.equipo_id,
              row.equipo_codigo,
              scope,
            );

            if (!inventoryItems.length && !workOrderVisible && !equipmentVisible) {
              return null;
            }

            if (!inventoryItems.length) {
              return {
                ...row,
                work_orders: linkedWorkOrders,
                work_order_count: linkedWorkOrders.length,
                work_order_titles: linkedWorkOrders.map((item: any) => item.label),
              };
            }

            const criticalCount = inventoryItems.filter(
              (item) => String(item.nivel || '').toUpperCase() === 'CRITICAL',
            ).length;
            const preventiveCount = inventoryItems.length - criticalCount;
            return {
              ...row,
              detalle: `${inventoryItems.length} material(es) en alerta de inventario.`,
              title: `Inventario · ${inventoryItems.length} materiales en alerta`,
              subtitle: `Criticos: ${criticalCount} · Preventivos: ${preventiveCount}`,
              payload_json: {
                ...payload,
                inventory_items: inventoryItems,
                total_materiales: inventoryItems.length,
                materiales_criticos: criticalCount,
                materiales_preventivos: preventiveCount,
              },
              work_orders: linkedWorkOrders,
              work_order_count: linkedWorkOrders.length,
              work_order_titles: linkedWorkOrders.map((item: any) => item.label),
            };
          })
          .filter((item): item is NonNullable<typeof item> => Boolean(item));
    const filtered = q.work_order_id
      ? scopedRows.filter((row: any) =>
          Array.isArray(row.work_orders)
            ? row.work_orders.some(
                (item: any) => String(item?.id || '') === String(q.work_order_id),
              )
            : String(row.work_order_id || '') === String(q.work_order_id),
        )
      : scopedRows;
    const total = filtered.length;
    const totalPages = total > 0 ? Math.ceil(total / limit) : 1;
    const paginated = filtered.slice((page - 1) * limit, page * limit);
    return this.wrap(paginated, 'Alertas listadas', {
      page,
      limit,
      total,
      totalPages,
    });
  }

  async getAlertasSummary(sucursalId?: string | null) {
    const response = await this.listAlertas({ page: 1, limit: 500 }, sucursalId);
    const enrichedRows = Array.isArray(response?.data) ? response.data : [];

    const totals = {
      total: enrichedRows.length,
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

    for (const row of enrichedRows) {
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
    const inventoryImportRunning =
      this.inventoryImportSuppressed || (await this.isInventoryImportRunning());
    const managedOrigins: AlertOrigin[] = inventoryImportRunning
      ? [
          'SYSTEM',
          'PROGRAMACION',
          'REPORTE_DIARIO',
          'ANALISIS_LUBRICANTE',
          'COMBUSTIBLE',
        ]
      : [
          'SYSTEM',
          'PROGRAMACION',
          'REPORTE_DIARIO',
          'ANALISIS_LUBRICANTE',
          'COMBUSTIBLE',
          'INVENTARIO',
        ];
    const candidates = await this.buildAlertCandidates({
      includeInventory: !inventoryImportRunning,
    });
    const stats = await this.syncAlertCandidates(candidates, { managedOrigins });
    return this.wrap(
      {
        source,
        inventory_import_running: inventoryImportRunning,
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


  private async resolveWorkOrderComponentContext(
    equipmentId?: string | null,
    componentId?: string | null,
  ) {
    if (!componentId) {
      return {
        component: null,
        componentId: null,
        componentName: null,
        componentOfficialName: null,
      };
    }
    const component = await this.findOneOrFail(this.equipoComponenteRepo, {
      id: componentId,
      is_deleted: false,
    });
    if (equipmentId && component.equipo_id !== equipmentId) {
      throw new BadRequestException(
        'El compartimiento seleccionado no pertenece al equipo de la orden.',
      );
    }
    return {
      component,
      componentId: component.id,
      componentName: component.nombre ?? null,
      componentOfficialName: component.nombre_oficial ?? component.nombre ?? null,
    };
  }

  private async applyBlockingRelationship(
    workOrder: WorkOrderEntity,
    blockedByWorkOrderId?: string | null,
    blockedReason?: string | null,
  ) {
    const normalizedBlockedById = String(blockedByWorkOrderId || '').trim() || null;
    const payload = {
      ...((workOrder.valor_json ?? {}) as Record<string, unknown>),
    };

    if (!normalizedBlockedById) {
      workOrder.blocked_by_work_order_id = null;
      workOrder.blocked_reason = String(blockedReason || '').trim() || null;
      if (this.normalizeWorkflowStatus(workOrder.status_workflow) === 'BLOCKED') {
        const restoreCandidate = this.normalizeWorkflowStatus(
          payload.workflow_before_block ?? 'PLANNED',
        );
        workOrder.status_workflow =
          restoreCandidate === 'BLOCKED' ? 'PLANNED' : restoreCandidate;
        workOrder.resumed_at = new Date();
      }
      workOrder.blocked_at = null;
      workOrder.valor_json = payload;
      return null;
    }

    if (workOrder.id && normalizedBlockedById === workOrder.id) {
      throw new BadRequestException(
        'Una orden de trabajo no puede bloquearse a si misma.',
      );
    }

    const blocker = await this.findOneOrFail(this.woRepo, {
      id: normalizedBlockedById,
      is_deleted: false,
    });

    workOrder.blocked_by_work_order_id = blocker.id;
    workOrder.blocked_reason =
      String(blockedReason || '').trim() || workOrder.blocked_reason || null;

    if (this.normalizeWorkflowStatus(blocker.status_workflow) !== 'CLOSED') {
      const currentStatus = this.normalizeWorkflowStatus(workOrder.status_workflow);
      if (currentStatus !== 'BLOCKED' && currentStatus !== 'CLOSED') {
        payload.workflow_before_block = currentStatus;
      }
      workOrder.status_workflow = 'BLOCKED';
      workOrder.blocked_at = workOrder.blocked_at ?? new Date();
      workOrder.resumed_at = null;
    } else if (this.normalizeWorkflowStatus(workOrder.status_workflow) === 'BLOCKED') {
      const restoreCandidate = this.normalizeWorkflowStatus(
        payload.workflow_before_block ?? 'IN_PROGRESS',
      );
      workOrder.status_workflow =
        restoreCandidate === 'BLOCKED' ? 'IN_PROGRESS' : restoreCandidate;
      workOrder.resumed_at = new Date();
    }

    workOrder.valor_json = {
      ...payload,
      blocking_work_order: {
        id: blocker.id,
        code: blocker.code,
        title: blocker.title,
        status_workflow: blocker.status_workflow,
      },
    };

    if (blocker.parent_work_order_id !== workOrder.id) {
      blocker.parent_work_order_id = workOrder.id ?? blocker.parent_work_order_id;
      await this.woRepo.save(blocker);
    }

    return blocker;
  }

  private async releaseBlockedWorkOrdersFor(blocker: WorkOrderEntity) {
    const blockedRows = await this.woRepo.find({
      where: {
        blocked_by_work_order_id: blocker.id,
        is_deleted: false,
      },
    });

    for (const row of blockedRows) {
      if (this.normalizeWorkflowStatus(row.status_workflow) !== 'BLOCKED') {
        continue;
      }
      const payload = {
        ...((row.valor_json ?? {}) as Record<string, unknown>),
      };
      const restoreCandidate = this.normalizeWorkflowStatus(
        payload.workflow_before_block ?? 'IN_PROGRESS',
      );
      row.status_workflow =
        restoreCandidate === 'BLOCKED' ? 'IN_PROGRESS' : restoreCandidate;
      row.resumed_at = new Date();
      row.valor_json = {
        ...payload,
        last_unblocked_by_work_order: {
          id: blocker.id,
          code: blocker.code,
          title: blocker.title,
        },
      };
      const saved = await this.woRepo.save(row);
      await this.appendWorkOrderHistory(
        saved.id,
        this.normalizeWorkflowStatus(saved.status_workflow),
        `Orden desbloqueada tras culminar OT anexada ${blocker.code}`,
        { fromStatus: 'BLOCKED' },
      );
      await this.publishInAppNotification({
        title: 'Orden desbloqueada',
        body: `${saved.code} ya puede continuar porque culmino la OT anexada ${blocker.code}.`,
        module: 'maintenance',
        entityType: 'work-order',
        entityId: saved.id,
        level: 'success',
      });
    }
  }

  async listWorkOrders(
    q: WorkOrderQueryDto,
    sucursalId?: string | null,
    actor?: RequestActorContext | null,
  ) {
    const fechaDesde = this.normalizeWorkOrderFilterDateBoundary(
      q.fecha_desde,
      'start',
    );
    const fechaHasta = this.normalizeWorkOrderFilterDateBoundary(
      q.fecha_hasta,
      'end',
    );
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
    if (fechaDesde) {
      qb.andWhere(
        `COALESCE(
          NULLIF((wo.valor_json->>'approved_at')::text, ''),
          NULLIF((wo.valor_json->>'processed_at')::text, ''),
          wo.closed_at::text,
          wo.started_at::text,
          wo.created_at::text
        )::timestamp >= :fechaDesde`,
        { fechaDesde },
      );
    }
    if (fechaHasta) {
      qb.andWhere(
        `COALESCE(
          NULLIF((wo.valor_json->>'approved_at')::text, ''),
          NULLIF((wo.valor_json->>'processed_at')::text, ''),
          wo.closed_at::text,
          wo.started_at::text,
          wo.created_at::text
        )::timestamp <= :fechaHasta`,
        { fechaHasta },
      );
    }
    qb.orderBy('wo.created_at', 'DESC');
    const rows = await this.filterWorkOrdersByScope(
      await qb.getMany(),
      await this.buildSucursalScopeContext(sucursalId),
    );
    return this.wrap(
      await Promise.all(rows.map((row) => this.enrichWorkOrder(row, actor))),
      'Work orders listadas',
    );
  }

  async getWorkOrder(
    id: string,
    sucursalId?: string | null,
    actor?: RequestActorContext | null,
  ) {
    const row = await this.findOneOrFail(this.woRepo, {
      id,
      is_deleted: false,
    });
    await this.assertWorkOrderVisibleForSucursal(row, sucursalId);
    return this.wrap(
      await this.enrichWorkOrder(row, actor),
      'Work order obtenida',
    );
  }

  async createWorkOrder(
    dto: CreateWorkOrderDto,
    actor?: RequestActorContext | null,
  ) {
    if (dto.equipment_id) await this.findEquipoOrFail(dto.equipment_id);
    const componentContext = await this.resolveWorkOrderComponentContext(
      dto.equipment_id ?? null,
      dto.equipo_componente_id ?? null,
    );
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
        equipo_componente_id: componentContext.componentId,
        equipo_componente_nombre: componentContext.componentName,
        equipo_componente_nombre_oficial: componentContext.componentOfficialName,
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
        blocked_by_work_order_id: null,
        blocked_reason: String(dto.blocked_reason || '').trim() || null,
        priority: dto.priority ?? 5,
        provider_type: dto.provider_type ?? 'INTERNO',
        maintenance_kind: dto.maintenance_kind ?? 'CORRECTIVO',
        safety_permit_required: dto.safety_permit_required ?? false,
        safety_permit_code: dto.safety_permit_code ?? null,
        vendor_id: dto.vendor_id ?? null,
        purchase_request_id: dto.purchase_request_id ?? null,
        requested_by: this.firstNonEmptyString(actor?.userId) ?? null,
        created_by: this.firstNonEmptyString(actor?.username) ?? null,
        updated_by: this.firstNonEmptyString(actor?.username) ?? null,
      });
      await this.applyBlockingRelationship(
        entity,
        dto.blocked_by_work_order_id ?? null,
        dto.blocked_reason ?? null,
      );
      this.applyWorkOrderAuditStamp(entity, actor, 'CREATED');
      if (this.normalizeWorkflowStatus(entity.status_workflow) === 'CLOSED') {
        this.applyWorkOrderAuditStamp(entity, actor, 'APPROVED', {
          action: 'CERRADA',
        });
      }
      this.applyWorkflowDates(entity, null, entity.status_workflow);

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

        if (created.blocked_by_work_order_id) {
      const blocker = await this.woRepo.findOne({
        where: { id: created.blocked_by_work_order_id, is_deleted: false },
      });
      if (blocker && blocker.parent_work_order_id !== created.id) {
        blocker.parent_work_order_id = created.id;
        await this.woRepo.save(blocker);
      }
    }

await this.appendWorkOrderHistory(
      created.id,
      this.normalizeWorkflowStatus(created.status_workflow),
      'Orden de trabajo creada',
      { changedBy: this.resolveActorLabel(actor) },
    );
    if (dto.alerta_id) {
      await this.syncAlertWorkOrderLink(
        dto.alerta_id,
        created,
        this.normalizeWorkflowStatus(created.status_workflow) === 'CLOSED'
          ? 'CERRADA'
          : 'EN_PROCESO',
      );
      await this.syncProgramacionWorkOrderLinkFromAlert(dto.alerta_id, created);
    }
    if (this.normalizeWorkflowStatus(created.status_workflow) === 'CLOSED') {
      await this.syncProgramacionExecutionFromLinkedWorkOrder(created);
    }
    const enriched = await this.enrichWorkOrder(created, actor);
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
      level: this.normalizeWorkflowStatus(created.status_workflow) === 'CLOSED' ? 'success' : 'info',
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

  async updateWorkOrder(
    id: string,
    dto: UpdateWorkOrderDto,
    actor?: RequestActorContext | null,
  ) {
    const wo = await this.findOneOrFail(this.woRepo, { id, is_deleted: false });
    const previousStatus = this.normalizeWorkflowStatus(wo.status_workflow);
    let resolvedPlanId = wo.plan_id ?? null;
    if (dto.procedimiento_id) {
      const synced = await this.syncPlanFromProcedimiento(dto.procedimiento_id);
      resolvedPlanId = synced.plan.id;
    }
    const componentContext = await this.resolveWorkOrderComponentContext(
      wo.equipment_id ?? null,
      dto.equipo_componente_id ?? wo.equipo_componente_id ?? null,
    );
    const nextWorkflowStatus = this.normalizeWorkflowStatus(
      dto.status_workflow ?? wo.status_workflow,
    );
    if (nextWorkflowStatus === 'CLOSED' && previousStatus !== 'CLOSED') {
      await this.assertCanCloseOrVoidWorkOrder(wo, actor, 'cerrar');
    }
    Object.assign(wo, {
      ...dto,
      plan_id: resolvedPlanId,
      equipo_componente_id: componentContext.componentId,
      equipo_componente_nombre: componentContext.componentName,
      equipo_componente_nombre_oficial: componentContext.componentOfficialName,
      blocked_reason:
        dto.blocked_reason !== undefined
          ? String(dto.blocked_reason || '').trim() || null
          : wo.blocked_reason,
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
      updated_by:
        this.firstNonEmptyString(actor?.username) ?? wo.updated_by ?? null,
    });
    wo.status_workflow = nextWorkflowStatus;
    if (nextWorkflowStatus === 'CLOSED') {
      this.applyWorkOrderAuditStamp(wo, actor, 'APPROVED', {
        action: 'CERRADA',
      });
    } else {
      this.applyWorkOrderAuditStamp(wo, actor, 'PROCESSED', {
        clearApproval: previousStatus === 'CLOSED',
      });
    }
    await this.applyBlockingRelationship(
      wo,
      dto.blocked_by_work_order_id !== undefined
        ? dto.blocked_by_work_order_id
        : wo.blocked_by_work_order_id,
      dto.blocked_reason !== undefined ? dto.blocked_reason : wo.blocked_reason,
    );
    this.applyWorkflowDates(wo, previousStatus, wo.status_workflow);
    const saved = await this.woRepo.save(wo);
    if (this.normalizeWorkflowStatus(saved.status_workflow) === 'CLOSED') {
      await this.releaseBlockedWorkOrdersFor(saved);
      await this.syncProgramacionExecutionFromLinkedWorkOrder(saved);
    }
    if (previousStatus !== saved.status_workflow) {
      await this.appendWorkOrderHistory(saved.id, saved.status_workflow, `Cambio de estado ${previousStatus} → ${saved.status_workflow}`, { fromStatus: previousStatus, changedBy: this.resolveActorLabel(actor) });
    } else {
      await this.appendWorkOrderHistory(saved.id, saved.status_workflow, 'Cabecera de OT actualizada', { fromStatus: previousStatus, changedBy: this.resolveActorLabel(actor) });
    }
    await this.syncAlertsForWorkOrder(saved);
    const enriched = await this.enrichWorkOrder(saved, actor);
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

  async deleteWorkOrder(id: string, actor?: RequestActorContext | null) {
    const wo = await this.findOneOrFail(this.woRepo, { id, is_deleted: false });
    await this.assertCanCloseOrVoidWorkOrder(wo, actor, 'anular');
    wo.is_deleted = true;
    wo.updated_by =
      this.firstNonEmptyString(actor?.username) ?? wo.updated_by ?? null;
    this.applyWorkOrderAuditStamp(wo, actor, 'APPROVED', {
      action: 'ANULADA',
    });
    await this.woRepo.save(wo);
    await this.detachProgramacionesFromWorkOrder(wo.id);
    await this.appendWorkOrderHistory(wo.id, this.normalizeWorkflowStatus(wo.status_workflow), 'Orden de trabajo eliminada lógicamente', { fromStatus: wo.status_workflow, changedBy: this.resolveActorLabel(actor) });
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
    actor?: RequestActorContext | null,
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
    this.applyWorkOrderAuditStamp(workOrder, actor, 'PROCESSED');
    workOrder.updated_by =
      this.firstNonEmptyString(actor?.username) ?? workOrder.updated_by ?? null;
    await this.woRepo.save(workOrder);
    await this.appendWorkOrderHistory(
      workOrderId,
      this.normalizeWorkflowStatus(workOrder.status_workflow),
      existing
        ? `Tarea sincronizada: ${taskDefinition.actividad}`
        : `Tarea registrada: ${taskDefinition.actividad}`,
      {
        fromStatus: workOrder.status_workflow,
        changedBy: this.resolveActorLabel(actor),
      },
    );
    return this.wrap(
      (await this.enrichWorkOrderTareas([created]))[0] ?? created,
      existing ? 'Tarea de OT sincronizada' : 'Tarea de OT creada',
    );
  }

  async updateWorkOrderTarea(
    id: string,
    dto: UpdateWorkOrderTareaDto,
    actor?: RequestActorContext | null,
  ) {
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
    this.applyWorkOrderAuditStamp(workOrder, actor, 'PROCESSED');
    workOrder.updated_by =
      this.firstNonEmptyString(actor?.username) ?? workOrder.updated_by ?? null;
    await this.woRepo.save(workOrder);
    await this.appendWorkOrderHistory(
      tarea.work_order_id,
      this.normalizeWorkflowStatus(workOrder.status_workflow),
      `Tarea actualizada: ${definition.actividad}`,
      {
        fromStatus: workOrder.status_workflow,
        changedBy: this.resolveActorLabel(actor),
      },
    );
    return this.wrap(
      (await this.enrichWorkOrderTareas([saved]))[0] ?? saved,
      'Tarea de OT actualizada',
    );
  }

  async deleteWorkOrderTarea(
    id: string,
    actor?: RequestActorContext | null,
  ) {
    const tarea = await this.findOneOrFail(this.woTareaRepo, {
      id,
      is_deleted: false,
    });
    tarea.is_deleted = true;
    await this.woTareaRepo.save(tarea);
    const workOrder = await this.findOneOrFail(this.woRepo, { id: tarea.work_order_id, is_deleted: false });
    this.applyWorkOrderAuditStamp(workOrder, actor, 'PROCESSED');
    workOrder.updated_by =
      this.firstNonEmptyString(actor?.username) ?? workOrder.updated_by ?? null;
    await this.woRepo.save(workOrder);
    await this.appendWorkOrderHistory(tarea.work_order_id, this.normalizeWorkflowStatus(workOrder.status_workflow), `Tarea eliminada: ${tarea.tarea_id}`, { fromStatus: workOrder.status_workflow, changedBy: this.resolveActorLabel(actor) });
    return this.wrap(true, 'Tarea de OT eliminada');
  }

  async uploadWorkOrderAdjunto(
    workOrderId: string,
    dto: UploadWorkOrderAdjuntoDto,
    actor?: RequestActorContext | null,
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
    this.applyWorkOrderAuditStamp(workOrder, actor, 'PROCESSED');
    workOrder.updated_by =
      this.firstNonEmptyString(actor?.username) ?? workOrder.updated_by ?? null;
    await this.woRepo.save(workOrder);
    await this.appendWorkOrderHistory(workOrderId, this.normalizeWorkflowStatus(workOrder.status_workflow), `Adjunto agregado: ${originalName}`, { fromStatus: workOrder.status_workflow, changedBy: this.resolveActorLabel(actor) });
    await this.writeSecurityLog({
      description: `[WO:${workOrderId}] Adjunto agregado ${originalName}`,
      typeLog: 'ADJUNTO',
    });
    return this.wrap(
      {
        ...created,
        ...this.buildWorkOrderAdjuntoLinks(workOrderId, created.id),
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
        ...this.buildWorkOrderAdjuntoLinks(workOrderId, row.id),
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
        ...this.buildWorkOrderAdjuntoLinks(workOrderId, adjuntoId),
        meta: adjunto.meta,
      },
      'Adjunto obtenido',
    );
  }

  async deleteWorkOrderAdjunto(
    workOrderId: string,
    adjuntoId: string,
    actor?: RequestActorContext | null,
  ) {
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
    this.applyWorkOrderAuditStamp(workOrder, actor, 'PROCESSED');
    workOrder.updated_by =
      this.firstNonEmptyString(actor?.username) ?? workOrder.updated_by ?? null;
    await this.woRepo.save(workOrder);
    await this.appendWorkOrderHistory(workOrderId, this.normalizeWorkflowStatus(workOrder.status_workflow), `Adjunto eliminado: ${adjunto.nombre ?? adjunto.id}`, { fromStatus: workOrder.status_workflow, changedBy: this.resolveActorLabel(actor) });
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

  async listConsumos(workOrderId: string, sucursalId?: string | null) {
    const workOrder = await this.findOneOrFail(this.woRepo, {
      id: workOrderId,
      is_deleted: false,
    });
    await this.assertWorkOrderVisibleForSucursal(workOrder, sucursalId);
    const scope = await this.buildSucursalScopeContext(sucursalId);
    const rows = await this.consumoRepo.find({
      where: { work_order_id: workOrderId, is_deleted: false },
      order: { id: 'DESC' },
    });
    const scopedRows = !scope
      ? rows
      : rows.filter((row) =>
          row.bodega_id
            ? scope.warehouseIds.has(String(row.bodega_id || '').trim())
            : false,
        );
    const { productMap, warehouseMap } = await this.buildInventoryCatalogMaps(
      scopedRows.map((row) => row.producto_id),
      scopedRows.map((row) => row.bodega_id || '').filter(Boolean),
    );
    const groupedTotals = new Map<
      string,
      { plannedQty: number; issuedQty: number; pendingQty: number }
    >();
    await Promise.all(
      scopedRows.map(async (row) => {
        const key = `${row.producto_id}|${row.bodega_id || ''}`;
        if (groupedTotals.has(key)) return;
        groupedTotals.set(
          key,
          await this.calculatePlannedAndIssuedMaterialTotals(
            workOrderId,
            row.producto_id,
            row.bodega_id || '',
          ),
        );
      }),
    );

    return this.wrap(
      scopedRows.map((row) => {
        const key = `${row.producto_id}|${row.bodega_id || ''}`;
        const totals = groupedTotals.get(key);
        return {
          ...this.mapConsumoWithCatalogs(row, productMap, warehouseMap),
          cantidad_reservada: totals?.plannedQty ?? this.toNumeric(row.cantidad, 0),
          cantidad_emitida: totals?.issuedQty ?? 0,
          cantidad_pendiente: totals?.pendingQty ?? this.toNumeric(row.cantidad, 0),
        };
      }),
      'Consumos listados',
    );
  }

  async listMaterialReservations(
    productoId: string,
    bodegaId: string,
    sucursalId?: string | null,
  ) {
    const normalizedProductoId = String(productoId || '').trim();
    const normalizedBodegaId = String(bodegaId || '').trim();

    if (!normalizedProductoId) {
      throw new BadRequestException('producto_id es obligatorio');
    }
    if (!normalizedBodegaId) {
      throw new BadRequestException('bodega_id es obligatorio');
    }

    await this.assertWarehouseVisibleForSucursal(
      normalizedBodegaId,
      sucursalId,
    );

    const [producto, bodega, reservas] = await Promise.all([
      this.findOneOrFail(this.productoRepo, {
        id: normalizedProductoId,
        is_deleted: false,
      }),
      this.findOneOrFail(this.bodegaRepo, {
        id: normalizedBodegaId,
        is_deleted: false,
      }),
      this.reservaRepo.find({
        where: {
          producto_id: normalizedProductoId,
          bodega_id: normalizedBodegaId,
          is_deleted: false,
        },
        order: { id: 'DESC' },
      }),
    ]);

    const workOrderIds = [
      ...new Set(
        reservas
          .map((row) => String(row.work_order_id || '').trim())
          .filter(Boolean),
      ),
    ];

    const workOrders = workOrderIds.length
      ? await this.woRepo.find({
          where: { id: In(workOrderIds), is_deleted: false },
        })
      : [];
    const workOrderMap = new Map(workOrders.map((row) => [row.id, row]));

    const equipmentIds = [
      ...new Set(
        workOrders
          .map((row) => String(row.equipment_id || '').trim())
          .filter(Boolean),
      ),
    ];
    const equipments = equipmentIds.length
      ? await this.equipoRepo.find({
          where: { id: In(equipmentIds), is_deleted: false },
        })
      : [];
    const equipmentMap = new Map(equipments.map((row) => [row.id, row]));

    const items = reservas.map((row) => {
      const workOrder = workOrderMap.get(row.work_order_id);
      const equipment = workOrder?.equipment_id
        ? equipmentMap.get(workOrder.equipment_id)
        : null;
      const workOrderLabel = workOrder
        ? [workOrder.code, workOrder.title].filter(Boolean).join(' - ')
        : `OT ${row.work_order_id}`;

      return {
        id: row.id,
        work_order_id: row.work_order_id,
        producto_id: row.producto_id,
        bodega_id: row.bodega_id,
        cantidad: this.toNumeric(row.cantidad, 0),
        estado: String(row.estado || 'RESERVADO').trim().toUpperCase(),
        work_order_code: workOrder?.code ?? null,
        work_order_title: workOrder?.title ?? null,
        work_order_status: workOrder
          ? this.normalizeWorkflowStatus(workOrder.status_workflow)
          : null,
        work_order_label: workOrderLabel,
        equipment_id: workOrder?.equipment_id ?? null,
        equipment_code: equipment?.codigo ?? null,
        equipment_name: equipment?.nombre ?? null,
        equipment_label:
          [equipment?.codigo, equipment?.nombre].filter(Boolean).join(' - ') ||
          null,
      };
    });

    const totalCantidad = items.reduce(
      (acc, item) => acc + this.toNumeric(item.cantidad, 0),
      0,
    );
    const activeCount = items.filter((item) => item.estado === 'RESERVADO').length;

    return this.wrap(
      {
        producto_id: producto.id,
        producto_codigo: producto.codigo ?? null,
        producto_nombre: producto.nombre ?? null,
        producto_label: this.buildProductoLabel(producto),
        bodega_id: bodega.id,
        bodega_codigo: bodega.codigo ?? null,
        bodega_nombre: bodega.nombre ?? null,
        bodega_label: this.buildBodegaLabel(bodega),
        total_reservas: items.length,
        reservas_activas: activeCount,
        total_cantidad: totalCantidad,
        items,
      },
      'Reservas de material listadas',
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

  async createConsumo(
    workOrderId: string,
    dto: CreateConsumoDto,
    actor?: RequestActorContext | null,
  ) {
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
    await this.upsertReservedMaterial(
      workOrderId,
      dto.producto_id,
      dto.bodega_id,
      dto.cantidad,
    );
    this.applyWorkOrderAuditStamp(workOrder, actor, 'PROCESSED');
    workOrder.updated_by =
      this.firstNonEmptyString(actor?.username) ?? workOrder.updated_by ?? null;
    await this.woRepo.save(workOrder);
    await this.appendWorkOrderHistory(workOrderId, this.normalizeWorkflowStatus(workOrder.status_workflow), `Consumo registrado para producto ${dto.producto_id} por ${dto.cantidad}`, { fromStatus: workOrder.status_workflow, changedBy: this.resolveActorLabel(actor) });
    await this.writeSecurityLog({
      description: `[WO:${workOrderId}] Consumo registrado producto ${dto.producto_id} cantidad ${dto.cantidad}`,
      typeLog: 'CONSUMO',
    });
    return this.wrap(
      {
        ...this.mapConsumoWithCatalogs(
          saved,
          new Map([[producto.id, producto]]),
          new Map([[bodega.id, bodega]]),
        ),
        cantidad_reservada: dto.cantidad,
        cantidad_emitida: 0,
        cantidad_pendiente: dto.cantidad,
      },
      'Consumo registrado',
    );
  }

  async listIssueMaterials(workOrderId: string, sucursalId?: string | null) {
    const workOrder = await this.findOneOrFail(this.woRepo, {
      id: workOrderId,
      is_deleted: false,
    });
    await this.assertWorkOrderVisibleForSucursal(workOrder, sucursalId);
    const scope = await this.buildSucursalScopeContext(sucursalId);
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
      entregas
        .map((entrega) => {
          const items = detalles
            .filter((detalle) => detalle.entrega_id === entrega.id)
            .filter((detalle) =>
              !scope
                ? true
                : scope.warehouseIds.has(String(detalle.bodega_id || '').trim()),
            )
            .map((detalle) =>
              this.mapIssueItemWithCatalogs(detalle, productMap, warehouseMap),
            );
          if (!items.length && scope) return null;
          return {
            ...entrega,
            items,
            total: items.reduce(
              (acc, item) =>
                acc + Number(item.costo_unitario || 0) * Number(item.cantidad || 0),
              0,
            ),
          };
        })
        .filter((item): item is NonNullable<typeof item> => Boolean(item)),
      'Salidas de materiales listadas',
    );
  }

  async issueMaterials(
    workOrderId: string,
    dto: IssueMaterialsDto,
    actor?: RequestActorContext | null,
  ) {
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
        let reserva = await qr.manager.findOne(ReservaStockEntity, {
          where: {
            work_order_id: workOrderId,
            producto_id: item.producto_id,
            bodega_id: item.bodega_id,
            estado: 'RESERVADO',
            is_deleted: false,
          },
        });
        if (!reserva || this.toNumeric(reserva.cantidad, 0) < item.cantidad) {
          reserva = await this.rebuildPendingReservaFromConsumos(
            workOrderId,
            item.producto_id,
            item.bodega_id,
            qr.manager,
          );
        }
        if (!reserva || this.toNumeric(reserva.cantidad, 0) < item.cantidad)
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
        const remainingReserved = this.toNumeric(reserva.cantidad, 0) - item.cantidad;
        reserva.cantidad = Math.max(remainingReserved, 0);
        reserva.estado = remainingReserved > 0 ? 'RESERVADO' : 'CONSUMIDO';
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
      }
      mov.total_costos = total;
      await qr.manager.save(mov);
      this.applyWorkOrderAuditStamp(workOrder, actor, 'PROCESSED');
      workOrder.updated_by =
        this.firstNonEmptyString(actor?.username) ?? workOrder.updated_by ?? null;
      await qr.manager.save(workOrder);
      await qr.commitTransaction();
      await this.appendWorkOrderHistory(workOrderId, this.normalizeWorkflowStatus(workOrder.status_workflow), `Salida de materiales registrada (${dto.items.length} items)`, { fromStatus: workOrder.status_workflow, changedBy: this.resolveActorLabel(actor) });
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
