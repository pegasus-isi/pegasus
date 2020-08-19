"use strict";

var jobBreakdownStats = { isLoaded: false };
var jobStats = { isLoaded: false };
var intStats = { isLoaded: false };

function render_workflow_summary_stats(dest, data) {
  var content = "";

  if (data.length == 0) {
    content += "No information available";
  }

  var content = "";
  content = '<table id="workflow_summary_stats_table">';
  content += "<tr>";
  content += "<th>Workflow Wall Time</th>";
  content += "<td>" + formatData(data["wall-time"]) + "</td>";
  content += "</tr>";
  content += "<tr>";
  content += "<th>Workflow Cumulative Job Wall Time</th>";
  content += "<td>" + formatData(data["cum-time"]) + "</td>";
  content += "</tr>";
  content += "<tr>";
  content += "<th>Cumulative Job Walltime as seen from Submit Side</th>";
  content += "<td>" + formatData(data["job-cum-time"]) + "</td>";
  content += "</tr>";
  content += "<tr>";
  content += "<th>Workflow Cumulative Badput Time</th>";
  content += "<td>" + formatData(data["cum-badput-time"]) + "</td>";
  content += "</tr>";
  content += "<tr>";
  content += "<th>Cumulative Job Badput Walltime as seen from Submit Side</th>";
  content += "<td>" + formatData(data["job-cum-badput-time"]) + "</td>";
  content += "</tr>";
  content += "<tr>";
  content += "<th>Workflow Retries</th>";
  content += "<td>" + formatData(data["retry-count"]) + "</td>";
  content += "</tr>";
  content += "</table>";

  dest.html(content);

  verticalTableInit("#workflow_summary_stats_table");
}

function render_workflow_stats(dest, all_data) {
  var content = "";

  var data = all_data.individual;

  if (data.length == 0) {
    content += "No information available";
  }

  content +=
    '<header class="ui-widget-header" style="padding: .3em;">This Workflow</header>';
  content += render_workflow_stats_table("individual", data) + "<br />";

  data = all_data.all;

  if (data.length == 0) {
    content += "No information available";
  }

  content +=
    '<header class="ui-widget-header" style="padding: .3em;">Entire Workflow</header>';
  content += render_workflow_stats_table("all", data);

  dest.html(content);

  $("#workflow_stats_individual_table").dataTable({
    jQueryUI: true,
    dom: '<"top"iflp<"clear">>rt<"bottom"iflp<"clear">>',
    ordering: false,
    searching: false,
    paging: false,
    info: false,
  });

  $("#workflow_stats_all_table").dataTable({
    jQueryUI: true,
    dom: '<"top"iflp<"clear">>rt<"bottom"iflp<"clear">>',
    ordering: false,
    searching: false,
    paging: false,
    info: false,
  });
}

function render_workflow_stats_table(type, data) {
  var content = "";
  content = '<table id="workflow_stats_' + type + '_table">';
  content += "<thead><tr>";
  content += "<th>Type</th>";
  content += "<th>Succeeded</th>";
  content += "<th>Failed</th>";
  content += "<th>Incomplete</th>";
  content += "<th>Total</th>";
  content += "<th>Retries</th>";
  content += "<th>Total + Retries</th>";
  content += "</tr></thead>";
  content += "<tbody>";

  content +=
    '<tr class="' +
    (data[0].total_failed_tasks === 0 ? "successful" : "failed") +
    '">';
  content += "<td>Tasks</td>";
  content += "<td>" + formatData(data[0].total_succeeded_tasks) + "</td>";
  content += "<td>" + formatData(data[0].total_failed_tasks) + "</td>";
  content += "<td>" + formatData(data[0].total_unsubmitted_tasks) + "</td>";
  content += "<td>" + formatData(data[0].total_tasks) + "</td>";
  content += "<td>" + formatData(data[0].total_task_retries) + "</td>";
  content += "<td>" + formatData(data[0].total_task_invocations) + "</td>";
  content += "</tr>";

  content +=
    '<tr class="' +
    (data[1].total_failed_jobs === 0 ? "successful" : "failed") +
    '">';
  content += "<td>Jobs</td>";
  content += "<td>" + formatData(data[1].total_succeeded_jobs) + "</td>";
  content += "<td>" + formatData(data[1].total_failed_jobs) + "</td>";
  content += "<td>" + formatData(data[1].total_unsubmitted_jobs) + "</td>";
  content += "<td>" + formatData(data[1].total_jobs) + "</td>";
  content += "<td>" + formatData(data[1].total_job_retries) + "</td>";
  content += "<td>" + formatData(data[1].total_job_invocations) + "</td>";
  content += "</tr>";

  content +=
    '<tr class="' +
    (data[2].total_failed_sub_wfs === 0 ? "successful" : "failed") +
    '">';
  content += "<td>Sub Workflows</td>";
  content += "<td>" + formatData(data[2].total_succeeded_sub_wfs) + "</td>";
  content += "<td>" + formatData(data[2].total_failed_sub_wfs) + "</td>";
  content += "<td>" + formatData(data[2].total_unsubmitted_sub_wfs) + "</td>";
  content += "<td>" + formatData(data[2].total_sub_wfs) + "</td>";
  content += "<td>" + formatData(data[2].total_sub_wfs_retries) + "</td>";
  content += "<td>" + formatData(data[2].total_sub_wfs_invocations) + "</td>";
  content += "</tr>";

  content += "</tbody></table>";

  return content;
}

function getJobBreakdownStats(url, container) {
  if (jobBreakdownStats.isLoaded) {
    return;
  }

  var ajaxOpt = {
    url: url,
    dataType: "json",
    error: function (xhr, textStatus, errorThrown) {
      alert("Error occurred: " + textStatus + xhr.responseText);
    },
    success: function (data, textStatus, xhr) {
      render_job_breakdown(container, data);
      jobBreakdownStats.isLoaded = true;
    },
  };

  $.ajax(ajaxOpt);
}

function render_job_breakdown(dest, data) {
  if (data.length == 0) {
    dest.html("No information available");
  }

  var content = "";
  content = '<table id="job_breakdown_stats_table">';
  content += "<thead><tr>";
  content += "<td colspan=2></td>";
  content += "<td></td><td colspan=4 align=center>Runtime (sec)</td>";
  content += "<td colspan=3 align=center>Memory (MB) ";
  content +=
    "<i class='fa fa-info-circle' data-toggle='tooltip' data-placement='top' title='Amount of physical memory (maxrss) used by the job as reported by kickstart'> </i></td>";
  content += "<td colspan=3 align=center>Avg. CPU (%) ";
  content +=
    "<i class='fa fa-info-circle' data-toggle='tooltip' data-placement='top' title='% of total duration of job that was spent running on cpu (stime+utime)/duration'> </i></td>";
  content += "</tr><tr>";
  content += "<th>Transformation ";
  content +=
    "<i class='fa fa-info-circle' data-toggle='tooltip' data-placement='right' title='The transformation associated with task in the workflow'> </i></th>";
  content += "<th>Type ";
  content +=
    "<i class='fa fa-info-circle' data-toggle='tooltip' data-placement='right' title='Grouping based on whether tasks succeeded or failed'> </i></th>";
  content += "<th>Count</th>";
  content += "<th>Min</th>";
  content += "<th>Max</th>";
  content += "<th>Mean</th>";
  content += "<th>Total</th>";
  content += "<th>Min</th>";
  content += "<th>Max</th>";
  content += "<th>Mean</th>";
  content += "<th>Min</th>";
  content += "<th>Max</th>";
  content += "<th>Mean</th>";
  content += "</tr></thead>";
  content += "<tbody>";

  for (var i = 0; i < data.length; ++i) {
    content += '<tr class="' + data[i][1].toLowerCase() + '">';
    for (var j = 0; j < data[i].length; ++j) {
      content += "<td>";
      content += formatData(data[i][j]);
      content += "</td>";
    }
    content += "</tr>";
  }

  content += "</tbody></table>";
  dest.html(content);

  $("#job_breakdown_stats_table").dataTable({
    jQueryUI: true,
    pagingType: "full_numbers",
    processing: true,
    serverSide: false,
    autoWidth: false,
  });

  $('[data-toggle="tooltip"]').tooltip();
}

function getJobStats(url, container) {
  if (jobStats.isLoaded) {
    return;
  }

  var ajaxOpt = {
    url: url,
    dataType: "json",
    error: function (xhr, textStatus, errorThrown) {
      alert("Error occurred: " + textStatus + " " + xhr.responseText);
    },
    success: function (data, textStatus, xhr) {
      render_job_stats(container, data);
      jobStats.isLoaded = true;
    },
  };

  $.ajax(ajaxOpt);
}

function render_job_stats(dest, data) {
  if (data.length == 0) {
    dest.html("No information available");
  }

  var content = "";
  content += '<table id="job_stats_table">';
  content += "<thead><tr>";
  content += '<th class="text-nowrap">Job</th>';
  content += '<th class="text-nowrap">Try</th>';
  content += '<th class="text-nowrap">Site</th>';
  content += '<th class="text-nowrap">Kickstart</th>';
  content += '<th class="text-nowrap">Multiplier</th>';
  content += '<th class="text-nowrap">Kickstart Multiplied</th>';
  content += '<th class="text-nowrap">CPU Time</th>';
  content += '<th class="text-nowrap">Post</th>';
  content += '<th class="text-nowrap">CondorQ Time</th>';
  content += '<th class="text-nowrap">Resource</th>';
  content += '<th class="text-nowrap">Runtime</th>';
  content += '<th class="text-nowrap">Seqexec</th>';
  content += '<th class="text-nowrap">Seqexec Delay</th>';
  content += '<th class="text-nowrap">Exitcode</th>';
  content += '<th class="text-nowrap">Host</th>';
  content += "</tr></thead>";
  content += "<tbody>";

  for (var i = 0; i < data.length; ++i) {
    content +=
      '<tr class="' + (data[i][13] === 0 ? "successful" : "failed") + '">';
    for (var j = 0; j < data[i].length; ++j) {
      content += "<td>";
      content += formatData(data[i][j]);
      content += "</td>";
    }
    content += "</tr>";
  }

  content += "</tbody></table>";
  dest.html(content);

  $("#job_stats_table").dataTable({
    scrollX: "100%",
    scrollCollapse: true,
    jQueryUI: true,
    pagingType: "full_numbers",
    processing: true,
    serverSide: false,
  });
}

function getIntegrityStats(url, container) {
  if (intStats.isLoaded) {
    return;
  }

  var ajaxOpt = {
    url: url,
    dataType: "json",
    error: function (xhr, textStatus, errorThrown) {
      alert("Error occurred: " + textStatus + " " + xhr.responseText);
    },
    success: function (data, textStatus, xhr) {
      render_integrity_stats(container, data);
      intStats.isLoaded = true;
    },
  };

  $.ajax(ajaxOpt);
}

function render_integrity_stats_table(type, data) {
  var content = "";
  content = '<table id="int_stats_' + type + '_table">';
  content += "<thead><tr>";
  content += "<th>Type</th>";
  content += "<th>File Type</th>";
  content += "<th>Count</th>";
  content += "<th>Duration</th>";
  content += "</tr></thead>";
  content += "<tbody>";

  for (var i = 0; i < data.length; ++i) {
    content += '<tr class="a">';
    content +=
      '<td style="text-transform: capitalize;">' + data[i].type + "</td>";
    content +=
      '<td style="text-transform: capitalize;">' + data[i].file_type + "</td>";
    content += "<td>" + formatData(data[i].count) + "</td>";
    content += "<td>" + formatData(data[i].duration) + "</td>";
    content += "</tr>";
  }

  content += "</tbody></table>";

  return content;
}

function render_integrity_stats(dest, all_data) {
  var content = "";

  var data = all_data.individual;

  if (data.length == 0) {
    content += "No information available";
  }

  content +=
    '<header class="ui-widget-header" style="padding: .3em;">This Workflow</header>';
  content += render_integrity_stats_table("individual", data) + "<br />";

  data = all_data.all;

  if (data.length == 0) {
    content += "No information available";
  }

  content +=
    '<header class="ui-widget-header" style="padding: .3em;">Entire Workflow</header>';
  content += render_integrity_stats_table("all", data);

  dest.html(content);

  $("#int_stats_individual_table").dataTable({
    jQueryUI: true,
    dom: '<"top"iflp<"clear">>rt<"bottom"iflp<"clear">>',
    ordering: false,
    searching: false,
    paging: false,
    info: false,
  });

  $("#int_stats_all_table").dataTable({
    jQueryUI: true,
    dom: '<"top"iflp<"clear">>rt<"bottom"iflp<"clear">>',
    ordering: false,
    searching: false,
    paging: false,
    info: false,
  });
}

function activateEventHandler(event, ui) {
  var tabIndex = ui.newHeader.attr("title");

  if (tabIndex == "workflow_stats") {
    return;
  } else if (tabIndex == "job_breakdown_stats") {
    getJobBreakdownStats(ui.newHeader.attr("href"), ui.newPanel);
  } else if (tabIndex == "job_stats") {
    getJobStats(ui.newHeader.attr("href"), ui.newPanel);
  } else if (tabIndex == "time_stats") {
    getTimeStats(ui.newHeader.attr("href"), ui.newPanel);
  } else if (tabIndex == "int_stats") {
    getIntegrityStats(ui.newHeader.attr("href"), ui.newPanel);
  } else {
    alert("Invalid accordian option " + tabIndex);
  }
}

function formatData(num) {
  if (typeof num === "number" && num % 1 !== 0) {
    return num.toFixed(3);
  }

  return num;
}

function debug(obj) {
  var i,
    s = "";
  for (i in obj) {
    s += i + " " + obj[i] + "\n";
  }

  alert(s);
}
