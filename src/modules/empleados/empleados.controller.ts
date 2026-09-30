import {
  Body,
  Controller,
  Delete,
  Get,
  Headers,
  Param,
  ParseUUIDPipe,
  Patch,
  Post,
  Query,
} from '@nestjs/common';
import {
  ApiOperation,
  ApiParam,
  ApiQuery,
  ApiResponse,
  ApiTags,
} from '@nestjs/swagger';
import {
  CreateEmpleadoDto,
  EmpleadoQueryDto,
  ImportarEmpleadosDto,
  UpdateEmpleadoDto,
} from './empleado.dto';
import { EmpleadosService } from './empleados.service';

/** Quien guarda, según la cabecera que el front manda en cada petición. */
function actor(usuario?: string) {
  return String(usuario ?? '').trim() || null;
}

@ApiTags('empleados')
@Controller('empleados')
export class EmpleadosController {
  constructor(private readonly service: EmpleadosService) {}

  @Get()
  @ApiOperation({ summary: 'Listar empleados con paginación y búsqueda' })
  @ApiResponse({ status: 200, description: 'Listado paginado' })
  listar(@Query() query: EmpleadoQueryDto) {
    return this.service.listar(query);
  }

  @Get('cargos')
  @ApiOperation({
    summary: 'Cargos ya registrados, sin repetir, para autocompletar',
  })
  @ApiQuery({ name: 'search', required: false, type: String })
  cargos(@Query('search') search?: string) {
    return this.service.listarCargos(search);
  }

  @Post('importar')
  @ApiOperation({
    summary:
      'Importar empleados desde las filas de un Excel (crea o actualiza por cédula)',
  })
  @ApiResponse({
    status: 201,
    description: 'Resumen: creados, actualizados, sin cambios y omitidos',
  })
  importar(
    @Body() dto: ImportarEmpleadosDto,
    @Headers('x-user-name') usuario?: string,
  ) {
    return this.service.importar(dto.empleados, actor(usuario));
  }

  @Get(':id')
  @ApiOperation({ summary: 'Obtener un empleado' })
  @ApiParam({ name: 'id', type: String, description: 'UUID del empleado' })
  @ApiResponse({ status: 404, description: 'Empleado no encontrado' })
  obtener(@Param('id', ParseUUIDPipe) id: string) {
    return this.service.obtener(id);
  }

  @Post()
  @ApiOperation({ summary: 'Crear un empleado' })
  @ApiResponse({ status: 201, description: 'Empleado creado' })
  crear(
    @Body() dto: CreateEmpleadoDto,
    @Headers('x-user-name') usuario?: string,
  ) {
    return this.service.crear(dto, actor(usuario));
  }

  @Patch(':id')
  @ApiOperation({ summary: 'Actualizar un empleado' })
  @ApiParam({ name: 'id', type: String, description: 'UUID del empleado' })
  @ApiResponse({ status: 404, description: 'Empleado no encontrado' })
  actualizar(
    @Param('id', ParseUUIDPipe) id: string,
    @Body() dto: UpdateEmpleadoDto,
    @Headers('x-user-name') usuario?: string,
  ) {
    return this.service.actualizar(id, dto, actor(usuario));
  }

  @Delete(':id')
  @ApiOperation({ summary: 'Eliminar un empleado (baja lógica)' })
  @ApiParam({ name: 'id', type: String, description: 'UUID del empleado' })
  @ApiResponse({ status: 404, description: 'Empleado no encontrado' })
  eliminar(
    @Param('id', ParseUUIDPipe) id: string,
    @Headers('x-user-name') usuario?: string,
  ) {
    return this.service.eliminar(id, actor(usuario));
  }
}
