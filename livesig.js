/*
    Copyright (C) 2013, 2014, 2015 Phil Wieland

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

var url_base = "/rail/livesig/";
var refresh_tick = 4096; /* ms between ticks */
var updating_timeout = 0;
var got_handle = 32767;
var req;
var req_cache = null;
var req_cache_k = '';
var req_cache_timeout = 0;
var feed_failed = 0;
var progress = 0;
var tick_timer;
var saved_caption = 'livesig';
var svg_doc;

// Coordinates of progress wheel
var progress_points;

// This contains <describer>/<describer>/<describer> ...
var describers;
var tiplocs;

var SLOTS = 32;
var displayed = new Array(SLOTS);
// Train cache.  Note that if cache_k[] == '' then the other values _v _e _u _c are INVALID.
var cache_k   = new Array(SLOTS);
var cache_v   = new Array(SLOTS);
var cache_e   = new Array(SLOTS); // Expiry time of cache entry
var cache_u   = new Array(SLOTS); // Most recent usage
var cache_c   = new Array(SLOTS); // Created.

// Debug
var qt = 0;
var qc = 0;
var lcd = 'None';

function startup()
{
   var i;
   refresh_count = 0;
   updating_timeout = 0;
   req_cache_timeout = 0;

   if(!document.getElementById('diagram').contentDocument)
   {
      document.getElementById('bottom-line').innerHTML = " Sorry, does not work in this browser with your current settings.";
      return;
   }
      
   svg_doc = document.getElementById('diagram').contentDocument;
   // Get co-ordinates
   var z = svg_doc.getElementById('progress').getAttribute('points').split(' ');
   progress_points = z[0].split(',');
   // Get describers and tiplocs
   describers = svg_doc.getElementById('info0').textContent;
   tiplocs = svg_doc.getElementById('info1').textContent;
   for(i=0; i < SLOTS; i++)
   {
      displayed[i] = '';
      cache_k[i] = '';
   }
   tick_timer = setInterval('tick()', refresh_tick);
}

function tick()
{
   //if ((typeof document.hidden !== "undefined") && (document.hidden)) return;
   display_debug();

   if(req_cache_timeout)
   {
      if(req_cache_timeout++ > 10)
      {
         // Cache update has timed out
         qt++; // Debug
         if(req_cache) req_cache.abort();
         req_cache = null;
         req_cache_timeout = 0;
         return;
      }
   }

   if(updating_timeout)
   {
      if(updating_timeout++ > 10)
      {
         // Update has timed out
         if(req) req.abort();
         req = null;
         updating_timeout = 0;
         svg_doc.getElementById('caption').style.stroke = 'red';
         svg_doc.getElementById('caption').textContent = " Failed to contact server.";
         got_handle = 32767;
      }
   }
   else
   {
      updating_timeout = 1;

      req = new XMLHttpRequest();

      req.onreadystatechange = function()
      {
         if(req.readyState == 4)
         {
            if(req.status == 200)
            {
               process_updates(req.responseText);
               req = null;
            }
            else
            {
               // Failed.  No action, leave for timeout to sort it out.
               req = null;
            }
         }
      }
      req.open('GET', url_base + 'U/' + got_handle + '/' + describers, true);
      req.send(null);
   }
}

function process_updates(text)
{
   // Note that we may get an update with a new handle but no updates in it, which should be handled quietly.
   var results = text.split("\n");

   var index = 1; // Index of first data line.

   if(text.substring(0, 8) != '<!DOCTYP')
   {
      got_handle = results[index - 1];
      while(results.length > index && results[index].length > 4 && (results[index].substr(2, 1) === 'b' || results[index].substr(2, 1) === 's'))
      {
         var parts = results[index++].split('|');
         if(parts.length > 1)
         {
            if(parts[0].substr(2,1) == 'b')
            {
               update_berth(parts[0], parts[1]);
            }
            else
            {
               update_signal(parts[0], parts[1]);
            }
         }
      }
   }
   
   update_panel();
   var caption = results[index].split('|');
   if((text.substring(0, 8) == '<!DOCTYP') || caption[0] > 0)
   {
      feed_failed = 10;
   }
   else
   {
      saved_caption = 'Updated to ' + caption[1] + ' by ' + caption[2] + ' ' + caption[3] + ' at ' + caption[4];
   }
   
   if(feed_failed)
   {
      feed_failed--;
      svg_doc.getElementById('caption').style.stroke = 'red';
      svg_doc.getElementById('caption').textContent = saved_caption + " - Data feed failed.";
   }
   else
   {
      svg_doc.getElementById('caption').style.stroke = 'black';
      svg_doc.getElementById('caption').textContent = saved_caption;
   }
   updating_timeout = 0;

   // Progress wheel
   progress += 40;
   if(progress > 359) progress -= 360;

   svg_doc.getElementById('progress').setAttribute('transform', 'rotate(' + progress + ',' + progress_points[0] + ',' + progress_points[1] + ')');
}

function update_berth(k, v)
{
   var i;
   if(svg_doc.getElementById(k))
   {
      var old = svg_doc.getElementById(k).textContent;
      if(old != '')
      {
         for(i=0; i < SLOTS; i++)
         {
            if(displayed[i] == old)
            {
               displayed[i] = '';
               i = SLOTS;
            }
         }
      }
      if(v != '' && v != 'SLOW' && v != 'FAST')
      {
         // Insert new value into displayed list.  If list is full, ignore the new value.
         // Note - If there are two locations with the same headcode, it will appear in the list twice.
         for(i=0; i < SLOTS; i++)
         {
            if(!displayed[i] || displayed[i] == '')
            {
               displayed[i] = v;
               i = SLOTS;
            }
         }
      }
      if(v == '')
      {
         svg_doc.getElementById(k).textContent = v;
         svg_doc.getElementById(k).style.opacity = '0.0';
         if(svg_doc.getElementById(k + 'off')) svg_doc.getElementById(k + 'off').style.opacity = '1.0';
      }
      else
      {
         svg_doc.getElementById(k).textContent = v;
         svg_doc.getElementById(k).style.opacity = '1.0';
         if(svg_doc.getElementById(k + 'off')) svg_doc.getElementById(k + 'off').style.opacity = '0.0';
      }
   }
}

function update_signal(k, v)
{
   var i;
   if(svg_doc.getElementById(k + 's'))
   {
      // It's a signal
      if(v === '')
      {
         svg_doc.getElementById(k + 's').style.fill = 'black';
      }
      else
      {
         svg_doc.getElementById(k + 's').style.fill = ((v === '1')?'#00ff00':'red');
      }
   }
   else if(svg_doc.getElementById(k + 'r'))
   {
      // It's a route
      svg_doc.getElementById(k + 'r').style.opacity = ((v === '1')?'1.0':'0.0');
   }
}

function update_panel()
{
   var i;
   var j;
   var last = '';
   var cache_result;
   var panel_row;

   displayed.sort();

   j = 0;
   for(i=0; i < SLOTS; i++)
   {
      if(displayed[i] != '' && displayed[i] != last)
      {
         last = displayed[i];
         panel_row = svg_doc.getElementById('info' + j);
         if(panel_row)
         {
            cache_result = cached(displayed[i]);
            panel_row.textContent = displayed[i] + ' ' + cache_result[0];
            if(cache_result[1])
            {
               panel_row.style.stroke = '#ff7700';
            }
            else
            {
               panel_row.style.stroke = '#ffff00';
            }
         }
         j++;
      }
   }
   for(;j < SLOTS; j++)
   {
      if(svg_doc.getElementById('info' + j))
      {
         svg_doc.getElementById('info' + j).textContent = '';
      }
   }
   display_debug();
}

function cached(k)
{
   // Return values:  [0] Description of train.  [1] Recent addition flag.
   var i;
   var now =  new Date().getTime();
   var result = ['', 1];
   for(i = 0; i < SLOTS; i++)
   {
      if(cache_k[i] == k)
      {
         // Cache hit.
         cache_u[i] = now;
         if(cache_e[i] < now)
         {
            // We've got a hit but it needs refreshing
            // Leave it in the cache and return it as the result, but request a reload.
            lcd = k; // Debug
            result = [cache_v[i], 0];
         }
         else
         {
            if(cache_c[i] > now - 8000)
            {
               return [cache_v[i], 1];
            }
            else
            {
               return [cache_v[i], 0];
            }
         }
      }
   }

   if(!req_cache)
   {
      qc++;
      req_cache = new XMLHttpRequest();
      req_cache_k = k;

      req_cache.onreadystatechange = function()
      {
         if(req_cache.readyState == 4)
         {
            if(req_cache.status == 200)
            {
               update_cache(req_cache_k,req_cache.responseText);
               req_cache = null;
               req_cache_timeout = 0;
            }
            else
            {
               // Failed.  No action, try again on next tick.
               req_cache = null;
               req_cache_timeout = 0;
            }
         }
      }
      req_cache.open('GET', url_base + 'Q/' + k + '/' + tiplocs, true);
      req_cache.send(null);
      req_cache_timeout = 1;
      return result;
   }
   return result;
}

function update_cache(k, v)
{
   // Called on async response from cache query.  May be a refresh of an existing entry
   var i;
   var now = new Date().getTime();
   var oldest = 0xfffffffffff;
   var chosen = 0;
   var hit = SLOTS;
   var empty = SLOTS;
   var results = v.split("\n");

   // Handle closed down web site
   if(v.substring(0, 8) == '<!DOCTYP')
   {
      return;
   }

   for(i = 0; i < SLOTS; i++)
   {
      if(cache_k[i] == k)
      {
         hit = i;
      }
      else if(cache_k[i] == '')
      {
         empty = i;
      }
      else if(cache_u[i] < oldest)
      {
         oldest = cache_u[i];
         chosen = i;
      }
   }

   if(hit < SLOTS)
   {
      chosen = hit;
   }
   else if(empty < SLOTS)
   {
      chosen = empty;
   }

   cache_k[chosen] = k;
   cache_v[chosen] = results[0];
   cache_u[chosen] = now;
   if(hit == SLOTS)
   {
      cache_c[chosen] = now;
   }
   if(results[0] == 'Not found.')
   {
      cache_e[chosen] = now + 16*60*1000; // 16 minutes
   }
   else
   {
      cache_e[chosen] = now + 64*60*1000; // 64 minutes
   }
   cache_e[chosen] += Math.floor(Math.random() * 128 * 1000);
}

function display_debug()
{
   var i;
   var cache_occupancy = 0;
   var display_count = 0;

   if(!svg_doc.getElementById('debug')) return;

   for(i = 0; i < SLOTS; i++)
   {
      if(cache_k[i] != '')
      {
         cache_occupancy++;
      }
      if(displayed[i] != '')
      {
         display_count++;
      }
   }

   svg_doc.getElementById('debug').textContent = 'QC' + qc + ' LQ' + req_cache_k + ' QT' + qt + ' RQT' + req_cache_timeout + ' D' + display_count + ' CO' + cache_occupancy + '/' + SLOTS + ' LCD(E)' + lcd + ' H' + got_handle;
}
