# Probe temperature sensor and drift calibration
#
# Copyright (C) 2024 Eric Callahan <arksine.code@gmail.com>
#
# This file may be distributed under the terms of the GNU GPLv3 license.
import logging
from threading import Lock
from . import manual_probe

KELVIN_TO_CELSIUS = -273.15

class TemperatureProbe:
    def __init__(self, config):
        self.name = config.get_name()
        self.printer = config.get_printer()
        self.gcode = self.printer.lookup_object("gcode")
        self.speed = config.getfloat("speed", None, above=0.)
        self.horizontal_move_z = config.getfloat(
            "horizontal_move_z", 2., above=0.
        )
        self.resting_z = config.getfloat("resting_z", .4, above=0.)
        # Setup temperature sensor
        smooth_time = config.getfloat("smooth_time", 2., above=0.)
        self.inv_smooth_time = 1. / smooth_time
        self.min_temp = config.getfloat(
            "min_temp", KELVIN_TO_CELSIUS, minval=KELVIN_TO_CELSIUS
        )
        self.max_temp = config.getfloat(
            "max_temp", 99999999.9, above=self.min_temp
        )
        pheaters = self.printer.load_object(config, "heaters")
        self.sensor = pheaters.setup_sensor(config)
        self.sensor.setup_minmax(self.min_temp, self.max_temp)
        self.sensor.setup_callback(self._temp_callback)
        pheaters.register_sensor(config, self)
        self.temp_lock = Lock()
        self.smoothed_temp = 0.
        self.last_temp_read_time = 0.
        self.measured_min = 99999999.
        self.measured_max = 0.

        # Calibration State
        self.cal_helper = None
        self.lift_speed = 5.
        self.probe_speed = 5.
        self.next_auto_temp = 99999999.
        self.target_temp = 0
        self.expected_count = 0
        self.sample_count = 0
        self.in_calibration = False
        self.step = 2.
        self.last_zero_pos = None
        self.total_expansion = 0
        self.start_pos = []

        # Register GCode Commands
        pname = self.name.split(maxsplit=1)[-1]
        self.gcode.register_mux_command(
            "PROBE_DRIFT_CALIBRATE", "PROBE", pname,
            self.cmd_PROBE_DRIFT_CALIBRATE,
            desc=self.cmd_PROBE_DRIFT_CALIBRATE_help
        )

    def register_calibration_helper(self, helper):
        self.cal_helper = helper

    def _temp_callback(self, read_time, temp):
        if not temp:
            return
        with self.temp_lock:
            time_diff = read_time - self.last_temp_read_time
            self.last_temp_read_time = read_time
            temp_diff = temp - self.smoothed_temp
            adj_time = min(time_diff * self.inv_smooth_time, 1.)
            self.smoothed_temp += temp_diff * adj_time
            self.measured_min = min(self.measured_min, self.smoothed_temp)
            self.measured_max = max(self.measured_max, self.smoothed_temp)
            st = self.smoothed_temp
            if self.in_calibration and st >= self.next_auto_temp:
                self.next_auto_temp = 99999999.
                self.printer.get_reactor().register_async_callback(
                    lambda e, gc=self.gcode: gc.run_script("PROBE_DRIFT_NEXT")
                )

    def get_temp(self, eventtime=None):
        return self.smoothed_temp, self.target_temp

    def _collect_sample(self, kin_pos, tool_zero_z):
        probe = self._get_probe()
        x_offset, y_offset, _ = probe.get_offsets()
        toolhead = self.printer.lookup_object("toolhead")
        cur_pos = toolhead.get_position()
        # Move to probe to sample collection position
        cur_pos[2] += self.horizontal_move_z
        toolhead.manual_move(cur_pos, self.lift_speed)
        cur_pos[0] -= x_offset
        cur_pos[1] -= y_offset
        move_speed = self.speed or self.probe_speed
        toolhead.manual_move(cur_pos, move_speed)
        speeds = (self.lift_speed, self.probe_speed, move_speed)
        return self.cal_helper.collect_sample(kin_pos, tool_zero_z, speeds)

    def _prepare_next_sample(self, last_temp, tool_zero_z):
        # Register our own abort command now that the manual
        # probe has finished and unregistered
        self.gcode.register_command(
            "ABORT", self.cmd_PROBE_DRIFT_ABORT,
            desc=self.cmd_PROBE_DRIFT_ABORT_help
        )
        # Move tool down to the resting position
        toolhead = self.printer.lookup_object("toolhead")
        cur_pos = toolhead.get_position()
        cur_pos[2] = tool_zero_z + self.resting_z
        toolhead.manual_move(cur_pos, self.probe_speed)
        cnt, exp_cnt = self.sample_count, self.expected_count
        self.next_auto_temp = last_temp + self.step
        self.gcode.respond_info(
            "%s: collected sample %d/%d at temp %.2fC, next sample scheduled "
            "at temp %.2fC"
            % (self.name, cnt, exp_cnt, last_temp, self.next_auto_temp)
        )

    def _manual_probe_finalize(self, kin_pos):
        if kin_pos is None:
            # Calibration aborted
            self._finalize_drift_cal(False)
            return
        if self.last_zero_pos is not None:
            z_diff = self.last_zero_pos[2] - kin_pos[2]
            self.total_expansion += z_diff
            logging.info(
                "Estimated Total Thermal Expansion: %.6f"
                % (self.total_expansion,)
            )
        self.last_zero_pos = kin_pos
        toolhead = self.printer.lookup_object("toolhead")
        tool_zero_z = toolhead.get_position()[2]
        try:
            last_temp = self._collect_sample(kin_pos, tool_zero_z)
        except Exception:
            self._finalize_drift_cal(False)
            raise
        self.sample_count += 1
        if last_temp >= self.target_temp:
            # Calibration Done
            self._finalize_drift_cal(True)
        else:
            try:
                self._prepare_next_sample(last_temp, tool_zero_z)
            except Exception:
                self._finalize_drift_cal(False)
                raise

    def _finalize_drift_cal(self, success, msg=None):
        self.next_auto_temp = 99999999.
        self.target_temp = 0
        self.expected_count = 0
        self.sample_count = 0
        self.step = 2.
        self.in_calibration = False
        self.last_zero_pos = None
        self.total_expansion = 0
        self.start_pos = []
        # Unregister Temporary Commands
        self.gcode.register_command("ABORT", None)
        self.gcode.register_command("PROBE_DRIFT_NEXT", None)
        self.gcode.register_command("PROBE_DRIFT_COMPLETE", None)
        try:
            self.cal_helper.finish_calibration(success)
        except self.gcode.error as e:
            success = False
            msg = str(e)
        if not success:
            msg = msg or "%s: calibration aborted" % (self.name,)
            self.gcode.respond_info(msg)

    def _get_probe(self):
        probe = self.printer.lookup_object("probe")
        if probe is None:
            raise self.gcode.error("No probe configured")
        return probe

    cmd_PROBE_DRIFT_CALIBRATE_help = (
        "Calibrate probe temperature drift compensation"
    )
    def cmd_PROBE_DRIFT_CALIBRATE(self, gcmd):
        if self.cal_helper is None:
            raise gcmd.error(
                "No calibration helper registered for [%s]"
                % (self.name,)
            )
        probe = self._get_probe()
        probe_name = probe.get_probe_name().split(maxsplit=1)[-1]
        short_name = self.name.split(maxsplit=1)[-1]
        if probe_name != short_name:
            raise self.gcode.error(
                "[%s] not linked to registered probe [%s]."
                % (self.name, probe.get_probe_name())
            )
        manual_probe.verify_no_manual_probe(self.printer)
        if self.in_calibration:
            raise gcmd.error(
                "Already in probe drift calibration. Use "
                "PROBE_DRIFT_COMPLETE or ABORT to exit."
            )
        cur_temp = self.smoothed_temp
        target_temp = gcmd.get_float("TARGET", above=cur_temp)
        step = gcmd.get_float("STEP", 2., minval=1.0)
        expected_count = int(
            (target_temp - cur_temp) / self.step + .5
        )
        if expected_count < 3:
            raise gcmd.error(
                "Invalid STEP and/or TARGET parameters resulted "
                "in too few expected samples: %d"
                % (expected_count,)
            )
        try:
            self.gcode.register_command(
                "PROBE_DRIFT_NEXT", self.cmd_PROBE_DRIFT_NEXT,
                desc=self.cmd_PROBE_DRIFT_NEXT_help
            )
            self.gcode.register_command(
                "PROBE_DRIFT_COMPLETE", self.cmd_PROBE_DRIFT_COMPLETE,
                desc=self.cmd_PROBE_DRIFT_NEXT_help
            )
        except self.printer.config_error:
            raise gcmd.error(
                "Auxiliary Probe Drift Commands already registered. Use "
                "PROBE_DRIFT_COMPLETE or ABORT to exit."
            )
        self.in_calibration = True
        self.cal_helper.start_calibration()
        self.target_temp = target_temp
        self.step = step
        self.sample_count = 0
        self.expected_count = expected_count
        # Caputure start position and begin initial probe
        toolhead = self.printer.lookup_object("toolhead")
        self.start_pos = toolhead.get_position()[:2]
        self.lift_speed = probe.get_lift_speed(gcmd)
        self.probe_speed = probe.get_probe_speed(gcmd)
        manual_probe.ManualProbeHelper(
            self.printer, gcmd, self._manual_probe_finalize
        )

    cmd_PROBE_DRIFT_NEXT_help = "Sample next probe drift temperature"
    def cmd_PROBE_DRIFT_NEXT(self, gcmd):
        manual_probe.verify_no_manual_probe(self.printer)
        self.next_auto_temp = 99999999.
        toolhead = self.printer.lookup_object("toolhead")
        # Lift and Move to nozzle back to start position
        curpos = toolhead.get_position()
        start_z = curpos[2]
        move_speed = self.speed or self.probe_speed
        # Move nozzle to the manual probing position
        curpos[2] += self.horizontal_move_z
        toolhead.manual_move(curpos, self.lift_speed)
        curpos[0] = self.start_pos[0]
        curpos[1] = self.start_pos[1]
        toolhead.manual_move(curpos, move_speed)
        curpos[2] = start_z
        toolhead.manual_move(curpos, self.probe_speed)
        self.gcode.register_command("ABORT", None)
        manual_probe.ManualProbeHelper(
            self.printer, gcmd, self._manual_probe_finalize
        )

    cmd_PROBE_DRIFT_COMPLETE_help = "Finish Probe Drift Calibration"
    def cmd_PROBE_DRIFT_COMPLETE(self, gcmd):
        manual_probe.verify_no_manual_probe(self.printer)
        self._finalize_drift_cal(self.sample_count >= 3)

    cmd_PROBE_DRIFT_ABORT_help = "Abort Probe Drift Calibration"
    def cmd_PROBE_DRIFT_ABORT(self, gcmd):
        self._finalize_drift_cal(False)

    def is_in_calibration(self):
        return self.in_calibration

    def get_status(self, eventtime=None):
        return {
            "temperature": self.smoothed_temp,
            "measured_min_temp": round(self.measured_min, 2),
            "measured_max_temp": round(self.measured_max, 2),
            "in_calibration": self.in_calibration,
            "estimated_expansion": self.total_expansion
        }

    def stats(self, eventtime):
        return False, '%s: temp=%.1f' % (self.name, self.smoothed_temp)


def load_config_prefix(config):
    return TemperatureProbe(config)
