"use strict";

// Globals

var jobDistribution = { choice: -1 };
var timeChart = { isLoaded: false };
var ganttChart = { isLoaded: false };

/*
 * Job Distribution Chart
 */

function initJobDistributionData(choice) {
  var data = jobDistribution.data;

  for (var i = 0; i < data.length; ++i) {
    if (choice == 1) {
      data[i].y = data[i].count.total;
    } else {
      data[i].y = data[i].time.total;
    }
  }
}

function jobDistributionGraphToggle(choice) {
  if (jobDistribution.choice == choice) {
    return;
  }

  jobDistribution.choice = choice;

  var opt = jobDistribution.opt;

  // Set Chart Title
  if (choice == 1) {
    opt.title.text = "Invocation Distribution by Count";
  } else {
    opt.title.text = "Invocation Distribution by Time";
  }

  initJobDistributionData(choice);
  opt.series[0].data = jobDistribution.data;

  opt.chart.animation = false;
  jobDistribution.chart = new Highcharts.Chart(opt);
}

function jobDistributionTooltipFormat() {
  var tip = "";
  tip += "<b>" + this.point.name + "</b><br/>";

  tip += "Total Count: " + this.point.count.total + "<br/>";
  tip += "Succeded Count: " + this.point.count.success + "<br/>";
  tip += "Failed Count: " + this.point.count.failure + "<br/>";

  tip += "Min Runtime: " + this.point.time.min + "<br/>";
  tip += "Average Runtime: " + this.point.time.avg + "<br/>";
  tip += "Max Runtime: " + this.point.time.max + "<br/>";
  tip += "Total Runtime: " + this.point.time.total + "<br/>";

  return tip;
}

function plotJobDistributionChart() {
  jobDistribution.opt = {
    chart: {
      renderTo: "job_distribution",
      plotBackgroundColor: null,
      plotBorderWidth: null,
      plotShadow: false,
      height: 600
    },
    title: {
      text: "Invocation Distribution by Count"
    },
    exporting: {
      enabled: true
    },
    credits: {
      enabled: false
    },
    tooltip: {
      formatter: jobDistributionTooltipFormat
    },
    plotOptions: {
      pie: {
        allowPointSelect: true,
        cursor: "pointer",
        showInLegend: true,
        dataLabels: {
          color: "#000000",
          formatter: function() {
            return "<b>" + this.point.name + ":</b> " + this.point.y;
          }
        }
      }
    },
    series: [
      {
        type: "pie",
        name: "Browser share",
        data: jobDistribution.data
      }
    ]
  };

  jobDistribution.chart = new Highcharts.Chart(jobDistribution.opt);
}

/*
 * Time Chart
 */

function timeChartToggle(choice) {
  var chart = timeChart.chart;
  var opt = timeChart.opt;

  if (choice == 1) {
    chart.setTitle({ text: "Time Chart by Jobs" });
    var count = chart.series[1].data;
    var runtime = chart.series[0].data;

    for (var i = 0; i < count.length; ++i) {
      count[i].update(timeChart.data[i].count.job, false);
      runtime[i].update(timeChart.data[i].total_runtime.job, false);
    }
  } else {
    chart.setTitle({ text: "Time Chart by Invocations" });
    var count = chart.series[1].data;
    var runtime = chart.series[0].data;

    for (var i = 0; i < count.length; ++i) {
      count[i].update(timeChart.data[i].count.invocation, false);
      runtime[i].update(timeChart.data[i].total_runtime.invocation, false);
    }
  }

  chart.redraw();
}

function getTimeChartCategories(data) {
  var categories = [];
  for (var i = 0; i < data.length; ++i) {
    categories.push(data[i].date_format);
  }

  return categories;
}

function getTimeChartJobCount(data) {
  var jobCounts = [];

  for (var i = 0; i < data.length; ++i) {
    jobCounts.push(data[i].count.job);
  }

  return jobCounts;
}

function getTimeChartJobRuntime(data) {
  var runtimes = [];

  for (var i = 0; i < data.length; ++i) {
    runtimes.push(data[i].total_runtime.job);
  }

  return runtimes;
}

function plotTimeChart(container) {
  var categories = timeChart.categories;
  var init_job_count = timeChart.jobCount;
  var init_job_time = timeChart.runtime;

  timeChart.opt = {
    chart: {
      renderTo: container
        .children()
        .first()
        .attr("id")
    },
    credits: {
      enabled: false
    },
    title: {
      text: "Time Chart by Jobs"
    },
    exporting: {
      enabled: true
    },
    tooltip: {
      formatter: function() {
        return (
          this.x + ": " + this.y + (this.series.name == "Count" ? "" : " secs")
        );
      }
    },
    xAxis: [
      {
        categories: categories,
        title: {
          text: "Time"
        }
      }
    ],
    yAxis: [
      {
        labels: {
          formatter: function() {
            return this.value;
          },
          style: {
            color: "#4572A7"
          }
        },
        title: {
          text: "Runtime (secs)",
          style: {
            color: "#4572A7"
          }
        }
      },
      {
        title: {
          text: "Count",
          style: {
            color: "#9A1919"
          }
        },
        labels: {
          style: {
            color: "#9A1919"
          }
        },
        opposite: true
      }
    ],
    series: [
      {
        name: "Runtime",
        color: Highcharts.getOptions().colors[0],
        type: "column",
        data: init_job_time
      },
      {
        name: "Count",
        color: Highcharts.getOptions().colors[1],
        type: "column",
        yAxis: 1,
        data: init_job_count
      }
    ]
  };

  timeChart.chart = new Highcharts.Chart(timeChart.opt);
}

function getTimeChart(url, container) {
  if (timeChart.isLoaded) {
    return;
  }

  var ajaxOpt = {
    url: url,
    dataType: "json",
    error: function(xhr, textStatus, errorThrown) {
      alert("Error occurred: " + textStatus + xhr.responseText);
    },
    success: function(data, textStatus, xhr) {
      container
        .children()
        .last()
        .buttonset();

      for (var i = 0; i < data.length; ++i) {
        var d = new Date(data[i].date_format * 1000 * 3600);
        data[i].date_format =
          d.getFullYear() +
          "-" +
          d.getMonth() +
          "-" +
          d.getDay() +
          " " +
          "Hour" +
          " " +
          d.getHours();
      }

      timeChart.data = data;
      timeChart.categories = getTimeChartCategories(data);
      timeChart.jobCount = getTimeChartJobCount(data);
      timeChart.runtime = getTimeChartJobRuntime(data);
      timeChart.isLoaded = true;

      plotTimeChart(container);
    }
  };

  $.ajax(ajaxOpt);
}

/*
 * Gantt Chart
 */

function getGanttChartCategories(data) {
  var categories = [];

  for (var i = 0; i < data.length; ++i) {
    categories.push(data[i].job_name);
  }

  return categories;
}

function getGanttChartSeries(data) {
  var series = [];

  var job = [];
  //var pre = [];
  //var condor = [];
  //var grid = [];
  //var exec = [];
  //var kickstart = [];
  //var post = [];
  var start = 0;

  for (var i = 0; i < data.length; ++i) {
    if (i == 0) {
      start = data[i].jobS;
    }
    job.push([
      data[i].jobS - start,
      data[i].jobS - start + data[i].jobDuration
    ]);
    //pre.push ([data [i].pre_start - start, data [i].pre_start - start + data [i].pre_duration]);
    //condor.push ([data [i].condor_start - start, data [i].condor_start - start + data [i].condor_duration]);
    //grid.push ([data [i].grid_start - start, data [i].grid_start - start + data [i].grid_duration]);
    //exec.push ([data [i].exec_start - start, data [i].exec_start - start + data [i].exec_duration]);
    //kickstart.push ([data [i].kickstart_start - start, data [i].kickstart_start - start + data [i].kickstart_duration]);
    //post.push ([data [i].post_start - start, data [i].post_start - start + data [i].post_duration]);
  }

  series.push({ name: "Job", data: job });
  //series.push ({name:"Condor", data: condor});
  //series.push ({name:"Grid", data: grid});
  //series.push ({name:"Pre", data: pre});
  //series.push ({name:"Exec", data: exec});
  //series.push ({name:"Kickstart", data: kickstart});
  //series.push ({name:"Post", data: post});

  return series;
}

function plotGanttChart(container) {
  var categories = ganttChart.categories;
  var series = ganttChart.series;

  ganttChart.opt = {
    chart: {
      renderTo: container.attr("id"),
      type: "columnrange",
      zoomType: "y",
      inverted: true
    },
    credits: {
      enabled: false
    },
    title: {
      text: "Workflow Execution Gantt Chart"
    },
    tooltip: {
      valueSuffix: " seconds"
    },
    xAxis: {
      categories: categories
    },
    yAxis: {
      min: 0,
      //type: 'logarithmic',
      title: {
        text: "Timeline (Seconds)"
      }
    },
    plotOptions: {
      columnrange: {
        //grouping: false, //Buggy
        dataLabels: {
          enabled: false
        }
      }
    },
    legend: {
      enabled: true
    },
    series: series
  };

  ganttChart.chart = new Highcharts.Chart(ganttChart.opt);
}

function getGanttChart(url, container) {
  if (ganttChart.isLoaded) {
    return;
  }

  var ajaxOpt = {
    url: url,
    dataType: "json",
    error: function(xhr, textStatus, errorThrown) {
      alert("Error occurred: " + textStatus + xhr.responseText);
    },
    success: function(data, textStatus, xhr) {
      container
        .children()
        .last()
        .buttonset();

      ganttChart.data = data;
      ganttChart.categories = getGanttChartCategories(data);
      ganttChart.series = getGanttChartSeries(data);
      ganttChart.isLoaded = true;

      plotGanttChart(container);
    }
  };

  $.ajax(ajaxOpt);
}

function activateEventHandler(event, ui) {
  var tabIndex = ui.newHeader.attr("title");

  if (tabIndex == "distribution_chart_container") {
    return;
  } else if (tabIndex == "time_chart_container") {
    getTimeChart(ui.newHeader.attr("href"), ui.newPanel);
  } else if (tabIndex == "gantt_chart") {
    getGanttChart(ui.newHeader.attr("href"), ui.newPanel);
  } else {
    alert("Invalid accordian option " + tabIndex);
  }
}
