import "virtual:uno.css";

import { ApexChartProps, SolidApexCharts } from 'solid-apexcharts';
import { createSignal, onMount } from 'solid-js'
import prettyBytes from "pretty-bytes";

const colors = [
  "#0ea5e9",
  "#1d4ed8",
  "#8b5cf6",
  "#d946ef",
  "#be185d",
  "#22c55e",
  "#f59e0b"
].reverse();

const chartOptions: ApexChartProps["options"]["chart"] = {
  background: "transparent",
  animations: {
    enabled: false,
  },
  toolbar: {
    show: false
  },
  zoom: {
    enabled: false
  },
}

const xaxisOptions: ApexChartProps["options"]["xaxis"] = {
  axisBorder: {
    show: true,
  },
  type: "datetime",
  labels: {
    style: {
      colors: "white"
    },
  }
}

const baseOptions: ApexChartProps["options"] = {
  grid: {
    show: false,
  },
  tooltip: {
    enabled: false,
  },
  dataLabels: {
    enabled: false
  },
  legend: {
    position: "top",
    horizontalAlign: 'right',
    labels: {
      colors: "white"
    }
  }
}

function LineChart(props: { alwaysShowLegend?: boolean, yFormatter?: (x: any) => string; title: string, series: { name: string, data: { x: number, y: number }[] }[] }) {
  const options = () => ({
    ...baseOptions,
    title: {
      text: props.title,
      style: {
        color: "white"
      }
    },
    stroke: {
      width: 2,
      curve: "straight",
    },
    legend: {
      ...baseOptions.legend,
      showForSingleSeries: props.alwaysShowLegend,
    },
    chart: {
      ...chartOptions,
    },
    xaxis: {
      ...xaxisOptions,
    },
    yaxis: {
      axisBorder: {
        show: true,
      },
      labels: {
        style: {
          colors: "white"
        },
        formatter: props.yFormatter,
      },

    },
  } satisfies ApexChartProps["options"]);

  const theseColors = [...colors];

  const series = () => ({
    list: [
      ...props.series.map(({ name, data }) => {
        return {
          name,
          data: data.map(({ x, y }) => ({
            x,
            y,
          })),
          color: theseColors.pop() ?? "#3b82f6",
        } satisfies ApexAxisChartSeries[0]
      }),
    ] satisfies ApexAxisChartSeries
  });

  return <SolidApexCharts
    type="line"
    width="100%"
    options={options()}
    series={series().list}
  />
}


function StackedAreaChart(props: { yFormatter: (x: any) => string; title: string, series: { name: string, data: { x: number, y: number }[] }[] }) {
  const options = () => ({
    ...baseOptions,
    title: {
      text: props.title,
      style: {
        color: "white"
      }
    },
    legend: {
      ...baseOptions.legend,
      showForSingleSeries: true,
    },
    stroke: {
      width: 2,
      curve: "straight",
    },
    chart: {
      ...chartOptions,
      stacked: true,
    },
    fill: {
      gradient: {
        opacityFrom: 1,
        opacityTo: 1,
        shadeIntensity: 0,
      }
    },
    xaxis: {
      ...xaxisOptions,
    },
    yaxis: {
      axisBorder: {
        show: true,
      },
      labels: {
        style: {
          colors: "white"
        },
        formatter: props.yFormatter,
      },
    },
  } satisfies ApexChartProps["options"]);

  const theseColors = [...colors];

  const series = () => ({
    list: [
      ...props.series.map(({ name, data }) => {
        const color = theseColors.pop() ?? "#3b82f6";

        return {
          name,
          data: data.map(({ x, y }) => ({
            x,
            y,
            color: color,
            fillColor: color,
            strokeColor: color,
          })),
          color: color,
        } satisfies ApexAxisChartSeries[0]
      }),
    ] satisfies ApexAxisChartSeries
  });

  return <SolidApexCharts
    type="area"
    width="100%"
    options={options()}
    series={series().list}
  />
}

function extractTimeseries(tableStatsMap: any, name: string) {
  return Object.entries<any>(tableStatsMap).map(([tableName, rows]) => ({
    name: tableName,
    data: (rows.find(r => r.row_key === name)?.columns.value[""] ?? []).map(({ timestamp, value: { F64: bytes } }) => ({
      x: new Date(timestamp / 1000 / 1000),
      y: bytes,
    }))
  })).filter(({ data }) => data.length > 0)
}

function App() {
  const [sysRows, _] = createSignal(JSON.parse(document.getElementById("system-metrics-data")!.textContent!));
  const [tableStatsMap, __] = createSignal(JSON.parse(document.getElementById("table-stats-data")!.textContent!));

  const cpu = () => (sysRows().find(x => x.row_key === "sys#cpu")?.columns.value[""] ?? []).map(({ timestamp, value: { F64: pct } }) => ({
    x: new Date(timestamp / 1000 / 1000),
    y: pct,
  }));

  const mem = () => (sysRows().find(x => x.row_key === "sys#mem")?.columns.value[""] ?? []).map(({ timestamp, value: { F64: bytes } }) => ({
    x: new Date(timestamp / 1000 / 1000),
    y: bytes,
  }));

  const writeBufferSize = () => (sysRows().find(x => x.row_key === "wbuf#size")?.columns.value[""] ?? []).map(({ timestamp, value: { F64: bytes } }) => ({
    x: new Date(timestamp / 1000 / 1000),
    y: bytes,
  }));

  const journalCount = () => (sysRows().find(x => x.row_key === "wal#len")?.columns.value[""] ?? []).map(({ timestamp, value: { F64, Byte } }) => ({
    x: new Date(timestamp / 1000 / 1000),
    y: Byte ?? F64, // NOTE: Byte has changed to F64
  }));

  const writeLatency = () => extractTimeseries(tableStatsMap(), "lat#write#batch");
  const rowReadLatency = () => extractTimeseries(tableStatsMap(), "lat#read#row");
  const prefixLatency = () => extractTimeseries(tableStatsMap(), "lat#read#pfx");
  const rowDeleteLatency = () => extractTimeseries(tableStatsMap(), "lat#del#row");
  const diskUsage = () => extractTimeseries(tableStatsMap(), "stats#du");
  const segmentCount = () => extractTimeseries(tableStatsMap(), "stats#seg_cnt");
  const rowCount = () => extractTimeseries(tableStatsMap(), "stats#row_cnt");
  const cellCount = () => extractTimeseries(tableStatsMap(), "stats#cell_cnt");
  const gcDeleteCount = () => extractTimeseries(tableStatsMap(), "gc#del_cnt");

  onMount(() => {
    setTimeout(() => window.location.reload(), 60 * 1000)
  });

  return (
    <div class="flex flex-col gap-10 mx-auto max-w-3xl">
      <div class="text-center text-xl text-white">
        Smoltable
      </div>
      <div class="grid sm:grid-cols-2 gap-3">
        <LineChart
          title="CPU usage (system)"
          yFormatter={(n) => `${Math.round(n)} %`}
          series={[
            {
              name: "CPU",
              data: cpu(),
            }
          ]}
        />
        <LineChart
          title="Memory usage (system)"
          yFormatter={prettyBytes}
          series={[
            {
              name: "Mem",
              data: mem(),
            }
          ]}
        />
        <LineChart
          title="Write buffer size"
          yFormatter={prettyBytes}
          series={[
            {
              name: "Write buffer size",
              data: writeBufferSize(),
            }
          ]}
        />
        <LineChart
          title="Journals count"
          yFormatter={x => Math.floor(x).toString()}
          series={[
            {
              name: "# journals",
              data: journalCount(),
            }
          ]}
        />
        <StackedAreaChart
          title="Disk usage"
          yFormatter={prettyBytes}
          series={[
            ...diskUsage(),
          ]}
        />
        <LineChart
          title="Disk segments count"
          yFormatter={x => {
            if (x < 1_000) {
              return x;
            }
            if (x < 1_000_000) {
              return `${(x / 1_000)}k`;
            }
            return `${(x / 1_000 / 1_000)}M`;
          }}
          series={[
            ...segmentCount(),
          ]}
        />
        <LineChart
          title="Row count"
          yFormatter={x => {
            if (x < 1_000) {
              return x;
            }
            if (x < 1_000_000) {
              return `${(x / 1_000)}k`;
            }
            return `${(x / 1_000 / 1_000)}M`;
          }}
          series={[
            ...rowCount(),
          ]}
        />
        <LineChart
          title="Cell count"
          yFormatter={x => {
            if (x < 1_000) {
              return x;
            }
            if (x < 1_000_000) {
              return `${(x / 1_000)}k`;
            }
            return `${(x / 1_000 / 1_000)}M`;
          }}
          series={[
            ...cellCount(),
          ]}
        />
        <LineChart
          alwaysShowLegend
          title="Write latency"
          yFormatter={x => {
            if (x < 1_000) {
              return `${x} µs`
            }
            if (x < 1_000_000) {
              return `${(x / 1000).toFixed(2)} ms`
            }
            return `${(x / 1000 / 1000).toFixed(2)} s`
          }}
          series={[
            ...writeLatency(),
          ]}
        />
        <LineChart
          alwaysShowLegend
          title="Row read latency"
          yFormatter={x => {
            if (x < 1_000) {
              return `${x} µs`
            }
            if (x < 1_000_000) {
              return `${(x / 1000).toFixed(2)} ms`
            }
            return `${(x / 1000 / 1000).toFixed(2)} s`
          }}
          series={[
            ...rowReadLatency(),
          ]}
        />
        <LineChart
          alwaysShowLegend
          title="Prefix scan latency"
          yFormatter={x => {
            if (x < 1_000) {
              return `${x} µs`
            }
            if (x < 1_000_000) {
              return `${(x / 1000).toFixed(2)} ms`
            }
            return `${(x / 1000 / 1000).toFixed(2)} s`
          }}
          series={[
            ...prefixLatency(),
          ]}
        />
        <LineChart
          alwaysShowLegend
          title="Delete row latency"
          yFormatter={x => {
            if (x < 1_000) {
              return `${x} µs`
            }
            if (x < 1_000_000) {
              return `${(x / 1000).toFixed(2)} ms`
            }
            return `${(x / 1000 / 1000).toFixed(2)} s`
          }}
          series={[
            ...rowDeleteLatency(),
          ]}
        />
        <LineChart
          alwaysShowLegend
          title="Cell GC delete count"
          yFormatter={x => {
            if (x < 1_000) {
              return x;
            }
            if (x < 1_000_000) {
              return `${(x / 1_000)}k`;
            }
            return `${(x / 1_000 / 1_000)}M`;
          }}
          series={[
            ...gcDeleteCount(),
          ]}
        />
      </div>
    </div>
  )
}

export default App
