# Support for eddy current based Z probes
#
# Copyright (C) 2021-2024  Kevin O'Connor <kevin@koconnor.net>
#
# This file may be distributed under the terms of the GNU GPLv3 license.
import math, bisect
from klippy import mcu
from . import ldc1612, probe, manual_probe

OUT_OF_RANGE = 99.9
TAP_NOISE_COMPENSATION = 0.8

# Linear regression (a*x + b) for list of (x,y) pairs
def simple_linear_regression(data):
    inv_count = 1. / len(data)
    x_avg = sum([d[0] for d in data]) * inv_count
    y_avg = sum([d[1] for d in data]) * inv_count
    x_var = sum([(d[0] - x_avg)**2 for d in data])
    xy_covar = sum([(d[0] - x_avg)*(d[1] - y_avg) for d in data])
    slope = xy_covar / x_var
    return slope, y_avg - slope*x_avg

# Tool for calibrating the sensor Z detection and applying that calibration
class EddyCalibration:
    def __init__(self, config):
        self.printer = config.get_printer()
        self.name = config.get_name()
        # Current calibration data
        self.cal_freqs = []
        self.cal_zpos = []
        cal = config.get("calibrate", None)
        if cal is not None:
            cal = [
                list(map(float, d.strip().split(":", 1)))
                for d in cal.split(",")
            ]
            self.load_calibration(cal)
        # Estimate toolhead velocity to change in frequency
        self.tap_threshold = self.tap_factor = 0.
        self.calc_frequency_rate()
        # Probe calibrate state
        self.probe_speed = 0.0
        # Register commands
        cname = self.name.split()[-1]
        gcode = self.printer.lookup_object("gcode")
        gcode.register_mux_command(
            "PROBE_EDDY_CURRENT_CALIBRATE",
            "CHIP",
            cname,
            self.cmd_EDDY_CALIBRATE,
            desc=self.cmd_EDDY_CALIBRATE_help,
        )

    def is_calibrated(self):
        return len(self.cal_freqs) > 2
    def calc_frequency_rate(self):
        count = len(self.cal_freqs)
        if count < 2:
            return
        data = []
        for i in range(count-1):
            f = self.cal_freqs[i]
            z = self.cal_zpos[i]
            f_next = self.cal_freqs[i+1]
            z_next = self.cal_zpos[i+1]
            chg_freq_per_dist = -((f_next - f) / (z_next - z))
            data.append((f, chg_freq_per_dist))
        raw_freq_rate = simple_linear_regression(data)
        freq_max_tap = -raw_freq_rate[1] / raw_freq_rate[0]
        z_max_tap = self.freq_to_height(freq_max_tap)
        # Pad rates to reduce impact of noise
        adj_slope = raw_freq_rate[0] * TAP_NOISE_COMPENSATION
        z_adj_tap = z_max_tap * TAP_NOISE_COMPENSATION
        freq_adj_tap = self.height_to_freq(z_adj_tap)
        self.tap_factor = adj_slope
        self.tap_threshold = freq_adj_tap
        # XXX
        logging.info("eddy tap threshold thr=%.3f,%.3f tf=%s",
                     self.tap_threshold,
                     self.freq_to_height(self.tap_threshold),
                     self.tap_factor)
    def load_calibration(self, cal):
        cal = sorted([(c[1], c[0]) for c in cal])
        self.cal_freqs = [c[0] for c in cal]
        self.cal_zpos = [c[1] for c in cal]

    def apply_calibration(self, samples):
        for i, (samp_time, freq, dummy_z) in enumerate(samples):
            pos = bisect.bisect(self.cal_freqs, freq)
            if pos >= len(self.cal_zpos):
                zpos = -99.9
            elif pos == 0:
                zpos = 99.9
            else:
                # XXX - could further optimize and avoid div by zero
                this_freq = self.cal_freqs[pos]
                prev_freq = self.cal_freqs[pos - 1]
                this_zpos = self.cal_zpos[pos]
                prev_zpos = self.cal_zpos[pos - 1]
                gain = (this_zpos - prev_zpos) / (this_freq - prev_freq)
                offset = prev_zpos - prev_freq * gain
                zpos = freq * gain + offset
            samples[i] = (samp_time, freq, round(zpos, 6))

    def height_to_freq(self, height):
        # XXX - could optimize lookup
        rev_zpos = list(reversed(self.cal_zpos))
        rev_freqs = list(reversed(self.cal_freqs))
        pos = bisect.bisect(rev_zpos, height)
        if pos == 0 or pos >= len(rev_zpos):
            raise self.printer.command_error(
                "Invalid probe_eddy_current height"
            )
        this_freq = rev_freqs[pos]
        prev_freq = rev_freqs[pos - 1]
        this_zpos = rev_zpos[pos]
        prev_zpos = rev_zpos[pos - 1]
        gain = (this_freq - prev_freq) / (this_zpos - prev_zpos)
        offset = prev_freq - prev_zpos * gain
        return height * gain + offset

    def do_calibration_moves(self, move_speed):
        toolhead = self.printer.lookup_object("toolhead")
        kin = toolhead.get_kinematics()
        move = toolhead.manual_move
        # Start data collection
        msgs = []
        is_finished = False

        def handle_batch(msg):
            if is_finished:
                return False
            msgs.append(msg)
            return True

        self.printer.lookup_object(self.name).add_client(handle_batch)
        toolhead.dwell(1.)
        # Move to each 40um and then each 5mm position
        samp_dist1 = 0.040
        samp_dist2 = 5.000
        req_zpos = [i*samp_dist1 for i in range(int(4.0 / samp_dist1) + 1)]
        req_zpos += [i*samp_dist2 for i in range(1, int(25.0 / samp_dist2) + 1)]
        start_pos = toolhead.get_position()
        times = []
        for zpos in req_zpos:
            # Move to next position (always descending to reduce backlash)
            hop_pos = list(start_pos)
            hop_pos[2] += zpos + 0.500
            move(hop_pos, move_speed)
            next_pos = list(start_pos)
            next_pos[2] += zpos
            move(next_pos, move_speed)
            # Note sample timing
            start_query_time = toolhead.get_last_move_time() + 0.050
            end_query_time = start_query_time + 0.100
            toolhead.dwell(0.200)
            # Find Z position based on actual commanded stepper position
            toolhead.flush_step_generation()
            kin_spos = {
                s.get_name(): s.get_commanded_position()
                for s in kin.get_steppers()
            }
            kin_pos = kin.calc_position(kin_spos)
            times.append((start_query_time, end_query_time, kin_pos[2]))
        toolhead.dwell(1.0)
        toolhead.wait_moves()
        # Finish data collection
        is_finished = True
        # Correlate query responses
        cal = {}
        step = 0
        for msg in msgs:
            for query_time, freq, old_z in msg["data"]:
                # Add to step tracking
                while step < len(times) and query_time > times[step][1]:
                    step += 1
                if step < len(times) and query_time >= times[step][0]:
                    cal.setdefault(times[step][2], []).append(freq)
        if len(cal) != len(times):
            raise self.printer.command_error(
                "Failed calibration - incomplete sensor data"
            )
        return cal

    def calc_freqs(self, meas):
        total_count = total_variance = 0
        positions = {}
        for pos, freqs in meas.items():
            count = len(freqs)
            freq_avg = float(sum(freqs)) / count
            positions[pos] = freq_avg
            total_count += count
            total_variance += sum([(f - freq_avg) ** 2 for f in freqs])
        return positions, math.sqrt(total_variance / total_count), total_count

    def post_manual_probe(self, kin_pos):
        if kin_pos is None:
            # Manual Probe was aborted
            return
        curpos = list(kin_pos)
        move = self.printer.lookup_object("toolhead").manual_move
        # Move away from the bed
        probe_calibrate_z = curpos[2]
        curpos[2] += 5.0
        move(curpos, self.probe_speed)
        # Move sensor over nozzle position
        pprobe = self.printer.lookup_object("probe")
        x_offset, y_offset, z_offset = pprobe.get_offsets()
        curpos[0] -= x_offset
        curpos[1] -= y_offset
        move(curpos, self.probe_speed)
        # Descend back to bed
        curpos[2] -= 5.0 - 0.050
        move(curpos, self.probe_speed)
        # Perform calibration movement and capture
        cal = self.do_calibration_moves(self.probe_speed)
        # Calculate each sample position average and variance
        positions, std, total = self.calc_freqs(cal)
        last_freq = 0.0
        for pos, freq in reversed(sorted(positions.items())):
            if freq <= last_freq:
                raise self.printer.command_error(
                    "Failed calibration - frequency not increasing each step"
                )
            last_freq = freq
        gcode = self.printer.lookup_object("gcode")
        gcode.respond_info(
            "probe_eddy_current: stddev=%.3f in %d queries\n"
            "The SAVE_CONFIG command will update the printer config file\n"
            "and restart the printer." % (std, total)
        )
        # Save results
        cal_contents = []
        for i, (pos, freq) in enumerate(sorted(positions.items())):
            if not i % 3:
                cal_contents.append("\n")
            cal_contents.append("%.6f:%.3f" % (pos - probe_calibrate_z, freq))
            cal_contents.append(",")
        cal_contents.pop()
        configfile = self.printer.lookup_object("configfile")
        configfile.set(self.name, "calibrate", "".join(cal_contents))

    cmd_EDDY_CALIBRATE_help = "Calibrate eddy current probe"

    def cmd_EDDY_CALIBRATE(self, gcmd):
        self.probe_speed = gcmd.get_float("PROBE_SPEED", 5.0, above=0.0)
        # Start manual probe
        manual_probe.ManualProbeHelper(
            self.printer, gcmd, self.post_manual_probe
        )


# Helper for implementing PROBE style commands
class EddyEndstopWrapper:
    REASON_SENSOR_ERROR = mcu.MCU_trsync.REASON_COMMS_TIMEOUT + 1

    def __init__(self, config, sensor_helper, calibration):
        self._printer = config.get_printer()
        self._sensor_helper = sensor_helper
        self._mcu = sensor_helper.get_mcu()
        self._calibration = calibration
        self._tap_height = config.getfloat('tap_height', None, minval=0.)
        self._use_tap = self._tap_height and calibration.is_calibrated()
        self._z_offset = config.getfloat('z_offset', minval=0.)
        self._dispatch = mcu.TriggerDispatch(self._mcu)
        self._trigger_time = 0.
        self._gather = None
        # XXX
        self._printer.register_event_handler('toolhead:drip', self._handle_drip)
        self._delayed_setup = 0.
    def _handle_drip(self, print_time, move):
        # XXX - mega hack - delayed sensor start to obtain toolhead accel_t
        if not self._use_tap or not self._delayed_setup:
            return
        pretap_freq = self._calibration.height_to_freq(self._tap_height)
        self._sensor_helper.setup_tap(
            self._delayed_setup, print_time + move.accel_t, pretap_freq,
            self._dispatch.get_oid(),
            mcu.MCU_trsync.REASON_ENDSTOP_HIT, self.REASON_SENSOR_ERROR,
            self._calibration.tap_threshold,
            self._calibration.tap_factor * move.cruise_v)
        self._delayed_setup = 0.
    # Interface for MCU_endstop
    def get_mcu(self):
        return self._mcu

    def add_stepper(self, stepper):
        self._dispatch.add_stepper(stepper)

    def get_steppers(self):
        return self._dispatch.get_steppers()

    def home_start(
        self, print_time, sample_time, sample_count, rest_time, triggered=True
    ):
        self._trigger_time = 0.0
        self._start_measurements(is_home=True)
        trigger_freq = self._calibration.height_to_freq(self._z_offset)
        trigger_completion = self._dispatch.start(print_time)
        if self._use_tap:
            # XXX
            self._delayed_setup = print_time
            return trigger_completion
        self._sensor_helper.setup_home(
            print_time,
            trigger_freq,
            self._dispatch.get_oid(),
            mcu.MCU_trsync.REASON_ENDSTOP_HIT,
            self.REASON_SENSOR_ERROR,
        )
        return trigger_completion

    def home_wait(self, home_end_time):
        self._dispatch.wait_end(home_end_time)
        trigger_time = self._sensor_helper.clear_home()
        self._stop_measurements(is_home=True)
        res = self._dispatch.stop()
        if res >= mcu.MCU_trsync.REASON_COMMS_TIMEOUT:
            raise self._printer.command_error(
                "Communication timeout during homing"
            )
        if res != mcu.MCU_trsync.REASON_ENDSTOP_HIT:
            return 0.0
        if self._mcu.is_fileoutput():
            return home_end_time
        self._trigger_time = trigger_time
        return trigger_time

    def query_endstop(self, print_time):
        return False  # XXX

    # Interface for ProbeEndstopWrapper
    def probing_move(self, pos, speed):
        # Perform probing move
        phoming = self._printer.lookup_object("homing")
        trig_pos = phoming.probing_move(self, pos, speed)
        if not self._trigger_time or self._use_tap:
            return trig_pos
        # Wait for samples to arrive
        start_time = self._trigger_time + 0.050
        end_time = start_time + 0.100
        reactor = self._printer.get_reactor()
        while 1:
            if self._samples and self._samples[-1]["data"][-1][0] >= end_time:
                break
            systime = reactor.monotonic()
            est_print_time = self._mcu.estimated_print_time(systime)
            if est_print_time > self._trigger_time + 1.0:
                raise self._printer.command_error(
                    "probe_eddy_current sensor outage"
                )
            reactor.pause(systime + 0.010)
        # Find position since trigger
        samples = self._samples
        self._samples = []
        samp_sum = 0.0
        samp_count = 0
        for msg in samples:
            data = msg["data"]
            if data[0][0] > end_time:
                break
            if data[-1][0] < start_time:
                continue
            for time, freq, z in data:
                if time >= start_time and time <= end_time:
                    samp_sum += z
                    samp_count += 1
        if not samp_count:
            raise self._printer.command_error(
                "Unable to obtain probe_eddy_current sensor readings"
            )
        halt_z = samp_sum / samp_count
        # Calculate reported "trigger" position
        toolhead = self._printer.lookup_object("toolhead")
        new_pos = toolhead.get_position()
        new_pos[2] += self._z_offset - halt_z
        return new_pos

    def multi_probe_begin(self):
        if not self._calibration.is_calibrated():
            raise self._printer.command_error(
                "Must calibrate probe_eddy_current first"
            )
        self._start_measurements()

    def multi_probe_end(self):
        self._stop_measurements()

    def probe_prepare(self, hmove):
        pass

    def probe_finish(self, hmove):
        pass

    def get_position_endstop(self):
        return self._z_offset


# Main "printer object"
class PrinterEddyProbe:
    def __init__(self, config):
        self.printer = config.get_printer()
        self.calibration = EddyCalibration(config)
        # Sensor type
        sensors = {"ldc1612": ldc1612.LDC1612}
        sensor_type = config.getchoice("sensor_type", {s: s for s in sensors})
        self.sensor_helper = sensors[sensor_type](config, self.calibration)
        # Probe interface
        self.probe = EddyEndstopWrapper(
            config, self.sensor_helper, self.calibration
        )
        self.printer.add_object("probe", probe.PrinterProbe(config, self.probe))

    def add_client(self, cb):
        self.sensor_helper.add_client(cb)


def load_config_prefix(config):
    return PrinterEddyProbe(config)
