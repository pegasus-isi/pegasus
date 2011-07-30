// JavaScript for Stampede dashboard.
    
$(document).ready(run)
function run() {
    var BAR_WIDTH = 500, // pixels
    BAR_SEGMENTS = { queued:"#B2DF8A",
                     successful:"#33A02C",
                     failed:"#E66101",
                     restarted:"#B2ABD2"}
    // Format #secs in a more readable form
    format_interval = function(sec) {
        dy = Math.floor(sec/86400)
        hr = Math.floor((sec%86400)/3600)
        mn = Math.floor((sec%3600)/60)
        sc = sec % 60        
        if (dy == 0 && hr == 0 && mn == 0) {
            return sc + "s"
        }
        if (dy == 0 && hr == 0) {
            return mn + "m "+ sc + "s"
        }
        if (dy == 0) {
            return hr +"h " + mn + "m " + sc + "s"
        }
        return dy + "d "+ hr +"h " + mn + "m " + sc + "s"
    }

    show_workflows = function(container) {
        var data = container.data,
            workflows = data.wf,
            now = new Date()
        // Set workflow num. ranges
        $("#wf_first").text("1")
        $("#wf_last").text(""+workflows.length)
        $("#wf_count").text(""+workflows.length)
        // Set timestamp vars
        $("#ts_now").text(now.format("yyyy-mm-dd hh:mm:ss tt"))
        $("#ts_delta").text(format_interval(Math.floor(now.getTime()/1000 - data.max_timestamp)))
        var tgt = $("#wf")
        for (var i=0; i < workflows.length; i++) {
            var jobs = workflows[i].jobs,
            subwf = workflows[i].subwf
            var scale = BAR_WIDTH / jobs.total,
            row;
            row = "<tr>"
            // Add sub-wf marker
            row += "<td class='xmark' style='width: 2em' align='center'>"
            if (subwf.length > 0) {
                row += "[" + subwf.length + "]</td>"
            }
            else {
                row += "[-]</td>"
            }
            // Add bar-graph
            row += "<td class='wfname'>" + workflows[i].name + "</td>"
            for (seg in BAR_SEGMENTS) {
                if (jobs[seg] <= 0) {
                    row += "<td class='wfbar' width='0'></td>"
                } 
                else {
                    var barwidth = scale * jobs[seg]
                    row += "<td class='wfbar' width='" + barwidth + "' "
                    row += "bgcolor='" + BAR_SEGMENTS[seg] + "' "
                    row += ">"+jobs[seg]+"</td>"
                }
            }
            row += "<td class='wftxt interval'>" + format_interval(workflows[i].wallclock) + "</td>"
            row += "</tr>"
            if (i==0) {
                hdr = "<tr>"
                hdr += "<td style='width: 2em'>Sub</td>"
                hdr += "<td class='wfname'>Name</td>"
                legend = "<span id='legend'>"
                for (seg in BAR_SEGMENTS) {
                    color = BAR_SEGMENTS[seg]
                    legend += "<span style='background: " + color
                    legend += ";'>&nbsp;&nbsp;&nbsp;</span>=" + seg
                    legend += "&nbsp;&nbsp;"
                }
                legend += "</span>"
                hdr += "<td colspan='4' width='" + BAR_WIDTH+"'>" + legend + "</td>"
                hdr += "<td class='wftxt interval'>Time</td>"
                hdr += "</tr>"
                tgt.append("<table class='wfstackhdr'><thead>" + hdr +
                           "</thead></table>")
            }
            tgt.append("<table class='wfstack'><tbody>" + row +
                       "</tbody></table>")
        }
    }
    refresh()
    $("#filter-form").submit(function() {
        refresh()
    })
}

// Refresh table
refresh = function() {
    $.getJSON("http://0.0.0.0:8080/workflows/", show_workflows)
}
             
