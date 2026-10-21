import { BadRequestException, Controller, Delete, Get, HttpCode, Param, ParseIntPipe, Post } from '@nestjs/common';
import { StressService } from './stress.service';

const RUN_ID_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;

@Controller('stress')
export class StressController {
  constructor(private readonly stressService: StressService) {}

  @Post(':runId/:id')
  @HttpCode(200)
  async write(@Param('runId') runId: string, @Param('id', ParseIntPipe) id: number): Promise<{ id: number }> {
    return this.stressService.write(this.validateRunId(runId), id);
  }

  @Get(':runId/keys')
  async listIds(@Param('runId') runId: string): Promise<{ runId: string; count: number; ids: number[] }> {
    return this.stressService.listIds(this.validateRunId(runId));
  }

  @Delete(':runId')
  async clear(@Param('runId') runId: string): Promise<{ deleted: number }> {
    return this.stressService.clear(runId === '*' ? runId : this.validateRunId(runId));
  }

  private validateRunId(runId: string): string {
    if (!RUN_ID_PATTERN.test(runId)) {
      throw new BadRequestException('runId must match [A-Za-z0-9_-]{1,64}');
    }
    return runId;
  }
}
