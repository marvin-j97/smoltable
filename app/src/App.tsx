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

function App() {
  const [sysRow, _] = createSignal(JSON.parse(document.getElementById("system-metrics-data")!.textContent!));
  const [tableRows, __] = createSignal(JSON.parse(document.getElementById("disk-usage-data")!.textContent!));
  const [latencyRows, ___] = createSignal(JSON.parse(document.getElementById("latency-data")!.textContent!));

  const cpu = () => sysRow().columns.stats.cpu.map(({ timestamp, value: { F64: pct } }) => ({
    x: new Date(timestamp / 1000 / 1000),
    y: pct,
  }));

  const mem = () => sysRow().columns.stats.mem.map(({ timestamp, value: { F64: bytes } }) => ({
    x: new Date(timestamp / 1000 / 1000),
    y: bytes,
  }));

  const journalCount = () => sysRow().columns.stats.wal_cnt.map(({ timestamp, value: { U8: y } }) => ({
    x: new Date(timestamp / 1000 / 1000),
    y,
  }));

  const tablesDiskUsage = () => tableRows().map((row) => ({
    name: row.row_key.replace("t#", "").replace("usr_", ""),
    data: (row.columns.stats.du ?? []).map(({ timestamp, value: { F64: bytes } }) => ({
      x: new Date(timestamp / 1000 / 1000),
      y: bytes,
    }))
  })).filter(({ data }) => data.length > 0);

  const segmentCounts = () => tableRows().map((row) => ({
    name: row.row_key.replace("t#", "").replace("usr_", ""),
    data: (row.columns.stats.seg_cnt ?? []).map(({ timestamp, value: { F64: count } }) => ({
      x: new Date(timestamp / 1000 / 1000),
      y: count,
    }))
  })).filter(({ data }) => data.length > 0);

  const rowCounts = () => tableRows().map((row) => ({
    name: row.row_key.replace("t#", "").replace("usr_", ""),
    data: (row.columns.stats.row_cnt ?? []).map(({ timestamp, value: { F64: count } }) => ({
      x: new Date(timestamp / 1000 / 1000),
      y: count,
    }))
  })).filter(({ data }) => data.length > 0);

  const cellCounts = () => tableRows().map((row) => ({
    name: row.row_key.replace("t#", "").replace("usr_", ""),
    data: (row.columns.stats.cell_cnt ?? []).map(({ timestamp, value: { F64: count } }) => ({
      x: new Date(timestamp / 1000 / 1000),
      y: count,
    }))
  })).filter(({ data }) => data.length > 0);

  const tablesWriteLatency = () => latencyRows().map((row) => ({
    name: row.row_key.replace("t#", "").replace("usr_", ""),
    data: (row.columns.lat["w"] ?? []).map(({ timestamp, value: { F64: bytes } }) => ({
      x: new Date(timestamp / 1000 / 1000),
      y: bytes,
    }))
  })).filter(({ data }) => data.length > 0);

  const tablesPointReadLatency = () => latencyRows().map((row) => ({
    name: row.row_key.replace("t#", "").replace("usr_", ""),
    data: (row.columns.lat["r#row"] ?? []).map(({ timestamp, value: { F64: bytes } }) => ({
      x: new Date(timestamp / 1000 / 1000),
      y: bytes,
    }))
  })).filter(({ data }) => data.length > 0);

  const tablesPrefixLatency = () => latencyRows().map((row) => ({
    name: row.row_key.replace("t#", "").replace("usr_", ""),
    data: (row.columns.lat["r#pfx"] ?? []).map(({ timestamp, value: { F64: bytes } }) => ({
      x: new Date(timestamp / 1000 / 1000),
      y: bytes,
    }))
  })).filter(({ data }) => data.length > 0);

  const tablesDeletesLatency = () => latencyRows().map((row) => ({
    name: row.row_key.replace("t#", "").replace("usr_", ""),
    data: (row.columns.lat["del#row"] ?? []).map(({ timestamp, value: { F64: bytes } }) => ({
      x: new Date(timestamp / 1000 / 1000),
      y: bytes,
    }))
  })).filter(({ data }) => data.length > 0);

  onMount(() => {
    setTimeout(() => window.location.reload(), 60 * 1000)
  });

  return (
    <div class="flex flex-col gap-10 mx-auto max-w-3xl">
      <div class="text-center text-xl text-white">
        Smoltable
      </div>
      <div class="grid grid-cols-2 gap-3">
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
            ...tablesDiskUsage(),
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
            ...segmentCounts(),
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
            ...rowCounts(),
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
            ...cellCounts(),
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
            ...tablesWriteLatency(),
          ]}
        />
        <LineChart
          alwaysShowLegend
          title="Point read latency"
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
            ...tablesPointReadLatency(),
          ]}
        />
        <LineChart
          alwaysShowLegend
          title="Scan latency"
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
            ...tablesPrefixLatency(),
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
            ...tablesDeletesLatency(),
          ]}
        />
      </div>
    </div>
  )
}

export default App
