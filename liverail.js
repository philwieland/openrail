/*
    Copyright (C) 2013, 2014, 2015, 2016, 2017, 2018, 2022 Phil Wieland

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

Build 3c15p

*/

var url_base = "/rail/liverail/";
var query_url_base = "/rail/query/";
var train_url_base = "/rail/livetrain/";
var refresh_tick = 1024; /* ms between ticks */
var refresh_period = 32000; /* ms */
var refresh_timeout_period = 32000; 
var timer_refresh = 0;
var timer_refresh_timeout = 0;
var tick_timer;
var visible = true;
var smart_update_req = null;

function search_onclick()
{
   ar_off();
   var result = url_base + "full" + '/' + document.getElementById("search_loc").value + '/' + document.getElementById("search_date").value;
   window.location = result;
}

function summary_onclick()
{
   ar_off();
   var result = url_base + "sum" + '/' + document.getElementById("search_loc").value + '/' + document.getElementById("search_date").value;
   window.location = result;
}

function freight_onclick()
{
   ar_off();
   var result = url_base + "frt" + '/' + document.getElementById("search_loc").value + '/' + document.getElementById("search_date").value;
   window.location = result;
}

function depart_onclick()
{
   ar_off();
   var result = url_base + "dep" + '/' + document.getElementById("search_loc").value + '/' + document.getElementById("search_date").value;
   window.location = result;
}

function as_search_onclick()
{
   ar_off();
   var result = url_base + "as" + '/' + document.getElementById("as-uid").value + '/' + document.getElementById("as-head").value;
   if(document.getElementById("as-this-week").checked)
   {
      result += '/w';
   }
   window.location = result;
}

function as_go_onclick()
{
   ar_off();
   var result = url_base + "train" + '/' + document.getElementById("as-g-id").value;
   window.location = result;
}

function as_rq_onclick()
{
   window.open(query_url_base, '_blank');
}

function status_onclick()
{
   ar_off();
   var result = url_base + "status";
   window.location = result;
}



function train_date_onclick(schedule_id)
{
   ar_off();
   var result = url_base + "train" + '/' + schedule_id + '/' + document.getElementById("train_date").value;
   window.location = result;
}

function train_date_onclick_train(UID)
{
   ar_off();
   var result = train_url_base + UID + '/' + document.getElementById("train_date").value;
   window.location = result;
}

function train_onclick(id_date)
{
   ar_off();
   var result = url_base + "train" + '/' + id_date;
   window.location = result;
}

function ar_onclick()
{
   if(document.getElementById("ar").checked)
   {
      timer_refresh = 1;
      show_progress(); 
      document.getElementById("progress").style.visibility='';
   }
   else
   {
      document.getElementById("progress").style.visibility='hidden';
      timer_refresh = 0;
   }
}

function ar_off()
{
   if(document.getElementById("ar"))
   {
      document.getElementById("ar").checked = false;
      document.getElementById("progress").style.visibility='hidden';
   }
   if(smart_update_req) smart_update_req.abort();
   smart_update_req = null;
   timer_refresh = 0;
   timer_refresh_timeout = 0;
}

function startup()
{
   tick_timer = setInterval(tick, refresh_tick);
   
   if(document.getElementById("display_handle") && document.getElementById("display_handle").value === "AROFF")
   {
      ar_off();
   }

   if(document.getElementById("ar") && document.getElementById("ar").checked)
   {
      // Refresh has been enabled.
      var url = document.URL;
      var url_parts = url.split('/');
      if(url_parts[5] == "sum" || url_parts[5] == "dep" || url_parts[5] == "panel")
      {
         // Smart update page.  Trigger an immediate update
         document.getElementById("progress").style.visibility='hidden';
         timer_refresh = 1;
      }
      else
      {
         // Dumb refresh page.  Set up refresh timer.
         timer_refresh = new Date().getTime() + refresh_period;
      }
   }
   else
   {
      if(document.getElementById("progress") )
      {
         document.getElementById("progress").style.visibility='hidden';
      }
   }
}

// Timer tick
function tick()
{
   var now = new Date().getTime();
   // Check if display is visible
   if ((typeof document.hidden !== "undefined") && (document.hidden))
   {
      if(visible)
      {
         visible = false;
      }
   }
   else
   {
      if(!visible)
      {
         // Becoming visible.
         if(document.getElementById("ar") && document.getElementById("ar").checked)
         {
            show_progress();
            visible = true;
         }
      }
   }
   if(visible && document.getElementById("ar") && document.getElementById("ar").checked)
   {
      if(timer_refresh && timer_refresh < now)
      {
         if(!smart_update_req)
         {
            show_progress();
            var url = document.URL;
            var altered_url = false;
            if(url.substr(url.length - 2, 2) != "/r") 
            { 
               url += "/r";
               altered_url = true;
            }
            var url_parts = url.split('/');
            if(url_parts[5] == "sum" || url_parts[5] == "dep" || url_parts[5] == "panel")
            {
               // Smart update
               smart_update(url);
               document.getElementById("progress").style.visibility='';
            }
            else
            {
               clearInterval(tick_timer);
               document.getElementById("bottom-line").innerHTML += "&nbsp;&nbsp;Refreshing...";
               if(altered_url)
               {
                  // Can't use reload because the current URL doesn't have /r on the end.
                  window.location = url;
               }
               else
               {
                  // Use reload so scroll position isn't changed.
                  window.location.reload();
               }
            }
            timer_refresh_timeout = now + refresh_timeout_period;
            timer_refresh = 0;
         }
      }
      else
      {
         show_progress();
      }
   }
   if(timer_refresh_timeout && timer_refresh_timeout < now)
   {
      // Smart update seems to have timed out.
      if(smart_update_req) smart_update_req.abort();
      smart_update_req = null;
      timer_refresh = 1;
      document.getElementById("bottom-line").innerHTML = "Failed to contact server.";
   }
}

function show_progress()
{
   var now = new Date().getTime();
   var progress = '<div style="background-color:white;display:block;width:100%;height:';
   var step = 25 - Math.round((timer_refresh - now) * 25 / refresh_period);
   if(step > 25) step = 25;
   if(step < 0 ) step = 0;
   progress += step;
   progress += 'px;"></div>';
   document.getElementById("progress").innerHTML = progress;
}

function smart_update(url)
{
   if(smart_update_req)
   {
      smart_update_req.abort();
      smart_update_req = null;
   }
   var update_url = url.replace('liverail/sum', 'liverail/sumu');
   update_url = update_url.replace('liverail/dep', 'liverail/depu');
   update_url = update_url.replace('liverail/panel', 'liverail/panelu');
   document.getElementById("bottom-line").innerHTML += "&nbsp;&nbsp;Updating...";

   smart_update_req = new XMLHttpRequest();
   smart_update_req.onreadystatechange = function()
      {
         if(smart_update_req.readyState == 4)
         {
            if(smart_update_req.status == 200)
            {
               process_smart_update_response(smart_update_req.responseText);
               smart_update_req = null;
               timer_refresh_timeout = 0;
               timer_refresh = new Date().getTime() + refresh_period;
               show_progress();
            }
            else
            {
               // Failed.  No action, leave for timeout to sort it out.
               smart_update_req = null;
            }
         }
      };
   smart_update_req.open('GET', update_url, true);
   smart_update_req.send(null);
}

function process_smart_update_response(result)
{
   var index = 1; // Index of first train data line.
   var results = result.split("\n");

   // Check display handle
   if(results.length < index || results[index-1] != document.getElementById('display_handle').value)
   {
      // Fetch has failed or page layout has changed or date has changed or software version has changed.  Reload whole page from scratch.
      if(result.substring(0, 8) == '<!DOCTYP')
      {
         // Web site shut down.
         var banner = document.getElementById("bottom-line").innerHTML;
         if(banner.indexOf('<br><br>') < 0)
         {
            // No banner
            banner = '';
         }
         else
         {
            banner = banner.substr(0, banner.indexOf('<br><br>') + 8);
         }
         document.getElementById("bottom-line").innerHTML = banner + '<span style=\"background-color:#ffbbbb;\">Data feed is shut down.</span>';

         return;
      }
      clearInterval(tick_timer);
      document.getElementById("bottom-line").innerHTML += "&nbsp;&nbsp;Refreshing...";
      var url = document.URL;
      if(url.substr(url.length - 2, 2) != "/r") { url += "/r"; }
      window.location = url;
      return;
   }

   // Process individual trains.
   while(results.length > index && results[index].length > 2 && results[index].substr(0, 2) == 'tr')
   {
      var parts = results[index].split('|');
      if(parts.length > 3)
      {
         document.getElementById(parts[0]).className = parts[1];
         document.getElementById(parts[0] + 'r').className = parts[2]; 
         document.getElementById(parts[0] + 'r').innerHTML = parts[3];
      }
      index++;
   }
   document.getElementById("bottom-line").innerHTML = results[index];
}
