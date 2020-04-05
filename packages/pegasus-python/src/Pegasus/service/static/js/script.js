"use strict";

$(function() {
  // Initialize Tooltips
  $('[data-toggle="tooltip"]').tooltip();
});

function clipboardTooltipInitialization() {
  var button = $(".copy-button");
  button
    .data("placement", "auto")
    .data("trigger", "hover")
    .attr("title", "Copy to clipboard")
    .tooltip();
  button.on("click", function(event) {
    var target = $(event.target);
    var text = target.data("clipboard-text");
    var copy = $("<input>");

    $("body").append(copy);
    copy.val(text).select();
    document.execCommand("copy");
    copy.remove();

    target
      .attr("title", "Copied!")
      .tooltip("fixTitle")
      .tooltip("show")
      .attr("title", "Copy to clipboard")
      .tooltip("fixTitle");
  });
}

function highChartsInitialization(colors) {
  if (colors == undefined)
    var colors = [
      "#337ab7",
      "#d9534f",
      "#5cb85c",
      "#f0ad4e",
      "#5bc0de",
      "#80699b",
      "#3d96ae",
      "#db843d",
      "#92a8cd",
      "#a47d7c",
      "#b5ca92"
    ];

  // Radialize the colors
  Highcharts.getOptions().colors = $.map(colors, function(color) {
    return {
      radialGradient: { cx: 0.5, cy: 0.5, r: 0.5 },
      stops: [
        [0, color],
        [
          1,
          Highcharts.Color(color)
            .brighten(-0.1)
            .get("rgb")
        ] // darken
      ]
    };
  });
}

function verticalTableInit(selector) {
  var table = $(selector);
  table.addClass("dataTable");
  table.attr("cellspacing", 0);
  table.attr("cellpadding", 5);
  $(selector + "> tbody > tr > th").each(function(index) {
    $(this).addClass("ui-state-default nowrap");
  });

  var isOdd = true;

  $(selector + "> tbody > tr").each(function(index) {
    if (isOdd) {
      $(this).addClass("odd");
    } else {
      $(this).addClass("even");
    }

    isOdd = !isOdd;
  });
}

/**
 * Display a table with based on status
 */
function verticalTableInitStatus(selector, status) {
  var table = $(selector);
  table.addClass("dataTable");
  table.attr("cellspacing", 0);
  table.attr("cellpadding", 5);
  $(selector + "> tbody > tr > th").each(function(index) {
    $(this).addClass("ui-state-default nowrap");
  });

  var isOdd = true;

  $(selector + "> tbody > tr").each(function(index) {
    if (isOdd) {
      $(this).addClass("odd");
    } else {
      $(this).addClass("even");
    }

    isOdd = !isOdd;

    if (status != null && status.toLowerCase() == "failed") {
      $(this).addClass("failed");
    } else if (status != null && status.toLowerCase() == "successful") {
      $(this).addClass("successful");
    } else {
      $(this).addClass("running");
    }
  });
}
