from datetime import timedelta

import apache_beam as beam
import time
from apache_beam.options.pipeline_options import PipelineOptions


def human_readable_window(window) -> str:
    """Formats a window object into a human readable string."""
    if isinstance(window, beam.window.GlobalWindow):
        return str(window)
    return f'{window.start.to_utc_datetime()} - {window.end.to_utc_datetime()}'


class PrintElementInfo(beam.DoFn):
    """Prints an element with its Window information."""

    def process(self, element, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        print(f'[{human_readable_window(window)}] {timestamp.to_utc_datetime()} -- {element}')
        yield element


@beam.ptransform_fn
def PrintWindowInfo(pcollection):
    """Prints the Window information with how many elements landed in that window."""

    class PrintCountsInfo(beam.DoFn):
        def process(self, num_elements, window=beam.DoFn.WindowParam):
            print(f'>> Window [{human_readable_window(window)}] has {num_elements} elements')
            yield num_elements

    return (
            pcollection
            | 'Count elements per window' >> beam.combiners.Count.Globally().without_defaults()
            | 'Print counts info' >> beam.ParDo(PrintCountsInfo())
    )


def to_unix_time(time_str: str, time_format='%Y-%m-%d %H:%M:%S') -> int:
    """Converts a time string into Unix time."""
    time_tuple = time.strptime(time_str, time_format)
    return int(time.mktime(time_tuple))


@beam.ptransform_fn
@beam.typehints.with_input_types(beam.pvalue.PBegin)
@beam.typehints.with_output_types(beam.window.TimestampedValue)
def AstronomicalEvents(pipeline):
    return (
            pipeline
            | 'Create data' >> beam.Create([
        ('2021-03-20 03:37:00', 'March Equinox 2021'),
        ('2021-04-26 22:31:00', 'Super full moon'),
        ('2021-05-11 13:59:00', 'Micro new moon'),
        ('2021-05-26 06:13:00', 'Super full moon, total lunar eclipse'),
        ('2021-06-20 22:32:00', 'June Solstice 2021'),
        ('2021-08-22 07:01:00', 'Blue moon'),
        ('2021-09-22 14:21:00', 'September Equinox 2021'),
        ('2021-11-04 15:14:00', 'Super new moon'),
        ('2021-11-19 02:57:00', 'Micro full moon, partial lunar eclipse'),
        ('2021-12-04 01:43:00', 'Super new moon'),
        ('2021-12-18 10:35:00', 'Micro full moon'),
        ('2021-12-21 09:59:00', 'December Solstice 2021'),
    ])
            | 'With timestamps' >> beam.MapTuple(
        lambda timestamp, element:
        beam.window.TimestampedValue(element, to_unix_time(timestamp))
    )
    )


# Lets see how the data looks like.
beam_options = PipelineOptions(flags=[], type_check_additional='all')
with beam.Pipeline(options=beam_options) as pipeline:
    (
            pipeline
            | 'Astronomical events' >> AstronomicalEvents()
            | 'Print element' >> beam.Map(print)
    )

# All elements fall into the GlobalWindow by default.
with beam.Pipeline() as pipeline:
    (
            pipeline
            | 'Astrolonomical events' >> AstronomicalEvents()
            | 'Print element info' >> beam.ParDo(PrintElementInfo())
            | 'Print window info' >> PrintWindowInfo()
    )

# Fixed-sized windows of approximately 3 months.
window_size = timedelta(days=3 * 30).total_seconds()  # in seconds
print(f'window_size: {window_size} seconds')

with beam.Pipeline() as pipeline:
    elements = (
            pipeline
            | 'Astronomical events' >> AstronomicalEvents()
            | 'Fixed windows' >> beam.WindowInto(beam.window.FixedWindows(window_size))
            | 'Print element info' >> beam.ParDo(PrintElementInfo())
            | 'Print window info' >> PrintWindowInfo()
    )

# Sliding windows of approximately 3 months every month.
window_size = timedelta(days=3 * 30).total_seconds()  # in seconds
window_period = timedelta(days=30).total_seconds()  # in seconds
print(f'window_size:   {window_size} seconds')
print(f'window_period: {window_period} seconds')

with beam.Pipeline() as pipeline:
    (
            pipeline
            | 'Astronomical events' >> AstronomicalEvents()
            | 'Sliding windows' >> beam.WindowInto(
        beam.window.SlidingWindows(window_size, window_period)
    )
            | 'Print element info' >> beam.ParDo(PrintElementInfo())
            | 'Print window info' >> PrintWindowInfo()
    )

# Sessions divided by approximately 1 month gaps.
gap_size = timedelta(days=30).total_seconds()  # in seconds
print(f'gap_size: {gap_size} seconds')

with beam.Pipeline() as pipeline:
    (
            pipeline
            | 'Astronomical events' >> AstronomicalEvents()
            | 'Session windows' >> beam.WindowInto(beam.window.Sessions(gap_size))
            | 'Print element info' >> beam.ParDo(PrintElementInfo())
            | 'Print window info' >> PrintWindowInfo()
    )
