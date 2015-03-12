"use strict";

$(function () {
    // Initialize Tooltips
    $('[data-toggle="tooltip"]').tooltip();
});

function clipboardTooltipInitialization()
{
    ZeroClipboard.config({hoverClass: "copy-button-hover"});

    var client = new ZeroClipboard(document.getElementsByClassName('copy-button'));
    client.on( 'ready', function(event)
    {
        var e = $("#global-zeroclipboard-html-bridge");
        e.data("placement", "auto").data("trigger", "hover").attr("title", "Copy to clipboard").tooltip();

        client.on('aftercopy', function (event)
        {
            e.attr("title", "Copied!").tooltip("fixTitle").tooltip("show").attr("title", "Copy to clipboard").tooltip("fixTitle");
        });
    });
}

function verticalTableInit (selector)
{
	var table = $(selector);
	table.addClass ('dataTable');
	table.attr ('cellspacing', 0);
	table.attr ('cellpadding', 5);
	$(selector + '> tbody > tr > th').each (function (index)
	{
		$(this).addClass ('ui-state-default nowrap');
	});

	var isOdd = true;

	$(selector + '> tbody > tr').each (function (index)
	{
		if (isOdd)
    	{
    		$(this).addClass ('odd');
    	}
    	else
    	{
    		$(this).addClass ('even');
    	}

    	isOdd = !isOdd;
	});
}

/**
 * Display a table with based on status
 */
function verticalTableInitStatus (selector, status)
{
    var table = $(selector);
    table.addClass ('dataTable');
    table.attr ('cellspacing', 0);
    table.attr ('cellpadding', 5);
    $(selector + '> tbody > tr > th').each (function (index)
    {
    	$(this).addClass ('ui-state-default nowrap');
    });

    var isOdd = true;

    $(selector + '> tbody > tr').each (function (index)
    {
    	if (isOdd)
    	{
    		$(this).addClass ('odd');
    	}
    	else
    	{
    		$(this).addClass ('even');
    	}

    	isOdd = !isOdd;

    	if (status != null && status.toLowerCase()  == 'failed')
    	{
    		$(this).addClass ('failed');
    	}
    	else if (status != null && status.toLowerCase()  == 'successful')
    	{
    		$(this).addClass ('successful');
    	}
    	else
    	{
    		$(this).addClass ('running');
    	}
   });
}
