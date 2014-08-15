/*
    Copyright (C) 2013, 2014 Phil Wieland

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

    phil@philwieland.com

*/

var url_base = "/rail/liverail/";
var refresh_tick = 3072; /* ms between ticks */
//var refresh_tick = 400; /* Testing only */
var refresh_period = 25; /* Number of ticks before refresh.  If not 25, css styles must be changed */
var refresh_count = 0;
var updating = 0;

function search_onclick()
{
   ar_off();
   result = url_base + "full" + '/' + document.getElementById("search_loc").value + '/' + document.getElementById("search_date").value;
   window.location = result;
}

function summary_onclick()
{
   ar_off();
   result = url_base + "sum" + '/' + document.getElementById("search_loc").value + '/' + document.getElementById("search_date").value;
   window.location = result;
}

function depart_onclick()
{
   ar_off();
   result = url_base + "dep" + '/' + document.getElementById("search_loc").value + '/' + document.getElementById("search_date").value;
   window.location = result;
}

function as_search_onclick()
{
   ar_off();
   result = url_base + "as" + '/' + document.getElementById("as-uid").value + '/' + document.getElementById("as-head").value;
   if(document.getElementById("as-this-week").checked)
   {
      result += '/w';
   }
   window.location = result;
}

function as_go_onclick()
{
   ar_off();
   result = url_base + "train" + '/' + document.getElementById("as-g-id").value;
   window.location = result;
}

function as_rq_onclick()
{
   ar_off();
   result = url_base + "as";
   window.location = result;
}

function status_onclick()
{
   ar_off();
   result = url_base + "status";
   window.location = result;
}

function train_date_onclick(schedule_id)
{
   // url may already have a date, and may have /r at the end.  Both to be dropped.
   // var url_parts = document.URL.split('/');
   // var url = "/";
   // for(var i = 3; i < 7; i++) url += url_parts[i] + '/';
   // url += document.getElementById("train_date").value;
   ar_off();
   result = url_base + "train" + '/' + schedule_id + '/' + document.getElementById("train_date").value;
   window.location = result;
}

function ar_onclick()
{
   if(document.getElementById("ar").checked)
   {
      refresh_count = refresh_period;
      show_progress(); 
      document.getElementById("progress").style.display='';
   }
   else
   {
      document.getElementById("progress").style.display='none';
   }
}

function ar_off()
{
   if(document.getElementById("ar"))
   {
      document.getElementById("ar").checked = false;
      document.getElementById("progress").style.display='none';
   }
}

function startup()
{
   setInterval('ar()', refresh_tick);
   refresh_count = 0;
   updating = 0;
   if(document.getElementById("ar") && document.getElementById("ar").checked)
   {
      // Refresh has been enabled.
 
      var url = document.URL;
      var url_parts = url.split('/');
      if(url_parts[5] == "sum" || url_parts[5] == "dep")
      {
         // trigger an immediate update
         document.getElementById("progress").style.display='none';
         refresh_count = refresh_period;
         ar();
      }
      else
      {
         show_progress(); 
         document.getElementById("progress").style.display='';
      }
   }
   else
   {
      if(document.getElementById("ar") )
      {
         document.getElementById("progress").style.display='none';
      }
   }
}

function ar()
{
   if(document.getElementById("ar") && document.getElementById("ar").checked)
   {
      if(++refresh_count < refresh_period)
      {
         show_progress();
      }
      else
      {
         if(!updating)
         {
            show_progress();
            updating = 1;
            var url = document.URL;
            if(url.substr(-2,2) != "/r") { url += "/r"; }
            var url_parts = url.split('/');
            if(url_parts[5] == "sum" || url_parts[5] == "dep")
            {
               // Smart update
               smart_update(url);
               document.getElementById("progress").style.display='';
            }
            else
            {
               window.location = url;
            }
            updating = 0;
         }
         else if(refresh_count > refresh_period * 8)
         {
            // Update seems to have timed out.  Try a reload.
            refresh_count = refresh_period;
            var url = document.URL;
            if(url.substr(-2,2) != "/r") { url += "/r"; }
            window.location = url;
         }
      }
   }
}

function show_progress()
{
   var progress = '<div style="background-color:white;display:block;width:100%%;height:';
   var step = Math.round(refresh_count * 25 / refresh_period);
   if(step > 25) step = 25;
   progress += step;
   progress += 'px;"></div>';
   document.getElementById("progress").innerHTML = progress;
}

function smart_update(url)
{
   var update_url = url.replace('/sum', '/sumu');
   update_url = update_url.replace('/dep', '/depu');
   document.getElementById("bottom-line").innerHTML += "&nbsp;&nbsp;Updating...";

   var req = new XMLHttpRequest();
   req.open('GET', update_url, false);
   req.send();
   var results = req.responseText.split("\n");

   var index = 1; // Index of first train data line.

   // Check display handle
   if(results.length < index || results[index-1] != document.getElementById('display_handle').value)
   {
      // Fetch has failed or page layout has changed or date has changed or software version has changed.  Reload whole page from scratch.
      window.location = url;
      return;
   }

   // Process individual trains.
   while(results.length > index && results[index].length > 2 && results[index].substr(0, 2) == 'tr')
   {
      var parts = results[index].split('|');
      if(parts.length > 2)
      {
         document.getElementById(parts[0]).className = parts[1];
         document.getElementById(parts[0]).innerHTML = parts[2]; // This doesn't work in IE9
      }
      index++;
   }
   refresh_count = 0;
   show_progress();

   document.getElementById("bottom-line").innerHTML = results[index];
}
