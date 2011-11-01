// JavaScript for Stampede dashboard.
    
var BAR_WIDTH = 500, // pixels
BAR_SEGMENTS = { queued:"#B2DF8A",
                 successful:"#33A02C",
                 failed:"#E66101",
                 restarted:"#B2ABD2"}

$(document).ready(run)

// Return the bar graph for a workflow
function add_bar_graph(workflows, i, jobs, scale, tgt) {
    var row = ""
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
    // header
    if (tgt != null && i==0) {
        hdr = "<tr>"
        hdr += "<td class='xmark'>Sub</td>"
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
    return row
}

// Show/hide on click
function set_wf_display(hide_subwf) {
    $("a.wfxpand").toggle(
        function() { 
            var wfid = $(this).attr('wf')
            var subwfs = $("tr.subwf[wf='"+wfid+"']")
            if (hide_subwf) {
                subwfs.show()
            }
            else {
                subwfs.hide()
            }
        },
        function() {
            var wfid = $(this).attr('wf')
            var subwfs = $("tr.subwf[wf='"+wfid+"']")
            if (hide_subwf) {
                subwfs.hide()
            }
            else {
                subwfs.show()
            }
        })
}

show_success = function(container, text_status, jqxhr) {
    jqxhr.success(show_workflows)
}

// Show all the workflow bars
show_workflows = function(container, text_status, jqxhr) {
    var data = container.data,
    workflows = data.wf,
    now = new Date()
    alert(""+data.wf)
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
        row += "<td class='xmark' align='center'>"
        if (subwf.length > 0) {
            row += "[<a class='wfxpand' href='#' wf='" + i + "'>" + subwf.length + "</a>]</td>"
        }
        else {
            row += "[-]</td>"
        }
        // Add row
        row += "<td class='wfname'>" + workflows[i].name + "</td>"
        row += add_bar_graph(workflows, i, jobs, scale, tgt)
        row += "</tr>"
        // Add sub-table if there are children
        if (subwf.length > 0) {
            subrows = ""
            for (var j=0; j < subwf.length; j++) {
                var jobs = subwf[j].jobs,
                scale = BAR_WIDTH / jobs.total
                subrows += "<tr class='subwf' wf='" + i + "'>"
                subrows += "<td class='xmark' align='center'>--</td>"
                subrows += "<td class='wfname'>" + subwf[j].name + "</td>"
                subrows += "<td><table width='" + BAR_WIDTH + "' class='subwf'><tr>"
                subrows += add_bar_graph(subwf, j, jobs, scale, null)
                subrows += "</tr></table></td></tr>"
            }
            row += subrows
        }
        tgt.append("<table class='wfstack'><tbody>" + row +
                       "</tbody></table>")
    }
    // Hide subworkflows
    var hide_subwf = true;
    if (hide_subwf) {
        $("tr.subwf").hide()
    }
    set_wf_display(hide_subwf)
}

// Refresh table
refresh = function() {
    $.ajax({
        url: "http://localhost:8080/workflows/",
        dataType: 'json',
        data: "",
        success: show_success
    });
}


function run() {
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

    // Submit action
    $("#filter-form").submit(function() {
        refresh()
    })

    // Draw, first time
    alert("draw1")
    refresh()
 }

