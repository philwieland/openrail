/*
    Copyright (C) 2013, 2014, 2015, 2017 Phil Wieland

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
var tick_period = 1024; /* ms between ticks */
var refresh_tick_limit = 4; /* Ticks between updates */ 
var refresh_tick_count = refresh_tick_limit;
var updating_timeout = 0;
var reset_handle = 'ZZZZZ';
var got_handle = reset_handle;
var req;
var req_cache = null;
var req_cache_k = '';
var req_cache_timeout = 0;
var feed_fault = 0; // 0-OK, 1-Latency problem, 2-Failed to contact server, 3-Server closed.
var progress = 0;
var tick_timer;
var saved_caption = 'livesig';
var svg_doc;
var visible = true;
var hidden_count;

// Global items deduced from SVG at startup
var progress_points;
var clock_object;
// This contains <describer>/<describer>/<describer> ...
var describers;
var tiplocs;
var tt1;
var tt2;
var ttb;

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
var dfull = 'None';
var flag = 0;

function startup()
{
   var i;
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
   clock_object = svg_doc.getElementById('clock');
   // Get describers and tiplocs
   describers = svg_doc.getElementById('info0').textContent;
   tiplocs = svg_doc.getElementById('info1').textContent;
   // "Tooltip" elements for berth and signal popups
   tt1 = svg_doc.getElementById('tt1');
   tt2 = svg_doc.getElementById('tt2');
   ttb = svg_doc.getElementById('ttb');
   for(i = 0; i < SLOTS; i++)
   {
      displayed[i] = '';
      cache_k[i] = '';
   }
   if(describers === '')
   {
      // Non-updating map
      var cap = svg_doc.getElementById('caption');
      cap.style.fill = 'black';
      cap.textContent = 'There is no live data on this diagram.';
      var b  = svg_doc.getElementById('banner');
      var b0 = svg_doc.getElementById('banner0');
      var b1 = svg_doc.getElementById('banner1');
      if(b ) {  b.textContent = ''; }
      if(b0) { b0.textContent = ''; }
      if(b1) { b1.textContent = ''; }
   }
   else
   {
      tick_timer = setInterval('tick()', tick_period);
   }
}

function tick()
{
   // Check if display is visible
   if (((typeof document.hidden !== "undefined") && (document.hidden)))
   {
      if(visible)
      {
         // Becoming hidden
         hidden_count = 0;
         visible = false;
      }
      hidden_count++;
      if(hidden_count === 4096)
      {
         // Been hidden for over an hour.  Clear expired data.
         var i;
         for(i = 0; i < SLOTS; i++)
         {
            displayed[i] = '';
            cache_k[i] = '';
            if(svg_doc.getElementById('info' + i))
            {
               svg_doc.getElementById('info' + i).textContent = '';
            }
         }
      }
   }
   else
   {
      if(!visible)
      {
         // Becoming visible
         got_handle = reset_handle;
         refresh_tick_count = refresh_tick_limit;
         visible = true;
      }
   }

   display_debug();
   if(visible) display_caption();

   if(req_cache_timeout)
   {
      if(req_cache_timeout++ > 10)
      {
         // Cache update has timed out
         qt++; // Debug count query request timeouts.
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
         feed_fault = 2;
         got_handle = reset_handle;
         progress_colour('red');
      }
   }

   if(++refresh_tick_count < refresh_tick_limit) return;
   refresh_tick_count = 0;

   // Update clock
   if(visible && clock_object)
   {
      var now = new Date();
      clock_object.textContent = now.toTimeString().substring(0, 5);
   }
   
   if(visible && !updating_timeout)
   {
      updating_timeout = 1;

      req = new XMLHttpRequest();

      req.onreadystatechange = function()
      {
         if(req.readyState == 4)
         {
            if(req.status == 200)
            {
               progress_colour('limegreen');
               process_updates(req.responseText);
               updating_timeout = 0;
               req = null;
            }
            else
            {
               // Failed.  No action, leave for timeout to sort it out.
               req = null;
               feed_fault = 2;
               progress_colour('red');
            }
         }
      }
      req.open('GET', url_base + 'U/' + got_handle + '/' + describers, true);
      req.send(null);
      progress_colour('orange');
   }
}

function process_updates(text)
{
   // Note that we may get an update with a new handle but no updates in it, which should be handled quietly.
   var results = text.split("\n");

   var index = 1; // Index of first data line.

   if(text.substring(0, 8) !== '<!DOCTYP')
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
            else if(parts[0].length == 5)
            {
               update_signals(parts[0], parts[1]);
            }
         }
      }

      var caption = results[index].split('|');
      if(caption[0] > 0) 
      {
         feed_fault = 1;
      }
      else
      {
         feed_fault = 0;
      }
      if(caption.length > 4)
      {
         saved_caption = 'Updated to ' + caption[1] + ' by ' + caption[2] + ' ' + caption[3] + ' at ' + caption[4];
      }

      var b  = svg_doc.getElementById('banner');
      var b0 = svg_doc.getElementById('banner0');
      var b1 = svg_doc.getElementById('banner1');
      if(caption.length > 6)
      {
         if(b ) {  b.textContent = caption[5] + '  ' + caption[6]; }
         if(b0) { b0.textContent = caption[5]; }
         if(b1) { b1.textContent = caption[6]; }
      }
      else
      {
         if(b ) {  b.textContent = ''; }
         if(b0) { b0.textContent = ''; }
         if(b1) { b1.textContent = ''; }
      }

      display_panel();
   }
   else
   {
      feed_fault = 3;
      progress_colour('red');
   }
   
   // Progress wheel
   progress += 40;
   if(progress > 359) progress -= 360;

   svg_doc.getElementById('progress').setAttribute('transform', 'rotate(' + progress + ',' + progress_points[0] + ',' + progress_points[1] + ')');
}

function display_caption()
{
   var cap = svg_doc.getElementById('caption');
   switch(feed_fault)
   {
   case 0: // Good
      cap.style.fill = 'black';
      cap.textContent = saved_caption;
      break;
   case 1: // Latency problem
      cap.style.fill = 'red';
      cap.textContent = saved_caption + " - Data feed problems.";
      break;
   case 2: // Connect timeout
      cap.style.fill = 'red';
      cap.textContent = 'Unable to contact server.  Retrying...';
      break;
   case 3: // Server closed.
      cap.style.fill = 'red';
      cap.textContent = 'Data feed is shut down.';
      break;
   }
}
function update_berth(k, v)
{
   var i;
   if(svg_doc.getElementById(k))
   {
      var old = svg_doc.getElementById(k).textContent;
      if(old !== '')
      {
         for(i = 0; i < SLOTS; i++)
         {
            if(displayed[i] === old)
            {
               displayed[i] = '';
               i = SLOTS * 2;
            }
         }
      }
      if(v !== '' && v !== 'SLOW' && v !== 'FAST')
      {
         // Insert new value into displayed list.  If list is full, ignore the new value.
         // Note - If there are two locations with the same headcode, it will appear in the list twice.
         for(i = 0; i < SLOTS; i++)
         {
            if(displayed[i] === '')
            {
               displayed[i] = v;
               i = SLOTS * 2;
            }
         }
         if(i == SLOTS) dfull = v;
      }
      if(v === '')
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

function update_signals(k, v)
{
   var i;
   for(i=0; i<8; i++)
   {
      if(v === '')
         update_signal('' + k + i, '');
      else
         update_signal('' + k + i, '' + ((v>>i) & 0x01));
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

function display_panel()
{
   var i;
   var j;
   var last = '';
   var cache_result;
   var panel_row;

   displayed.sort();

   j = 0;
   for(i = 0; i < SLOTS; i++)
   {
      if(displayed[i] !== '' && displayed[i] !== last)
      {
         last = displayed[i];
         panel_row = svg_doc.getElementById('info' + j);
         if(panel_row)
         {
            cache_result = cached(displayed[i]);
            /* panel_row.textContent = i + ' ' + displayed[i] + '-' + cache_result[0]; */
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
      if(cache_k[i] === k)
      {
         // Cache hit.
         cache_u[i] = now;
         if(cache_e[i] < now)
         {
            // We've got a hit but it needs refreshing
            // Leave it in the cache and return it as the result, but request a reload.
            lcd = k; // Debug Last cache entry timed out
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
            if(req_cache.status == 200 && req_cache.responseText.substring(0, 8) !== '<!DOCTYP')
            {
               update_cache(req_cache_k,req_cache.responseText);
               req_cache = null;
               req_cache_timeout = 0;
               if(visible) display_panel();
            }
            else
            {
               // Failed, or site shut down.  No action, try again on next tick.
               req_cache = null;
               req_cache_timeout = 0;
            }
         }
      }
      req_cache.open('GET', url_base + 'Q/' + k + '/' + tiplocs, true);
      req_cache.send(null);
      req_cache_timeout = 1;
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

   for(i = 0; i < SLOTS; i++)
   {
      if(cache_k[i] === k)
      {
         hit = i;
      }
      else if(cache_k[i] === '')
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
   if(results[0] === 'Not found.')
   {
      cache_e[chosen] = now + 8*60*1000; // 8 minutes.
   }
   else
   {
      cache_e[chosen] = now + 64*60*1000; // 64 minutes.
   }
   cache_e[chosen] += Math.floor(Math.random() * 256 * 1000); // Up to 256 seconds.
   
}

function display_debug()
{
   if(!svg_doc.getElementById('debug')) return;

   var i;
   var cache_occupancy = 0;
   var display_count = 0;

   flag++;
   flag %= 10;

   for(i = 0; i < SLOTS; i++)
   {
      if(cache_k[i] !== '')
      {
         cache_occupancy++;
      }
      if(displayed[i] !== '')
      {
         display_count++;
      }
   }

   svg_doc.getElementById('debug').textContent = 'QC' + qc + ' LQ' + req_cache_k + ' QT' + qt + ' RQT' + req_cache_timeout + ' D' + display_count + ' Df' + dfull + ' CO' + cache_occupancy + ' LCD' + lcd + ' H' + got_handle + ' Ff' + feed_fault + ' ' + flag%10;
}

function tt_on(evt, text)
{
   tt1.firstChild.data = text;
   tt2.firstChild.data = '';
   if(!evt.currentTarget.id)
   {
   }
   else if(evt.currentTarget.id.substring(2,3) === 'b')
   {
      var bid = evt.currentTarget.id.substring(3,7);
      var des = evt.currentTarget.id.substring(0,2);
      tt1.firstChild.data = 'Describer ' + des + ' Berth ' + bid;
      var occ_id = evt.currentTarget.id.substring(0,7);
      var headcode = svg_doc.getElementById(occ_id).textContent;
      if(headcode.length === 4 && headcode !== 'FAST' && headcode !== 'SLOW')
      {
         var cache_result = cached(headcode);
         tt2.firstChild.data = headcode + ' ' + cache_result[0];
      }
   }
   else if(evt.currentTarget.id.substring(2,3) === 's')
   {
      tt1.firstChild.data = 'Signal ' + text;
   }

   length = tt1.getComputedTextLength();
   if(tt2.getComputedTextLength() > length) length = tt2.getComputedTextLength();
   if(evt.clientX + length > 1240) 
   {
      tt1.setAttribute('x',1260-length-11);
      tt2.setAttribute('x',1260-length-11);
      ttb.setAttribute('x',1260-length-14);
   }
   else
   {
      tt1.setAttribute('x',evt.clientX+11);
      tt2.setAttribute('x',evt.clientX+11);
      ttb.setAttribute('x',evt.clientX+8);
   }
   tt1.setAttribute('y',evt.clientY+27);
   tt2.setAttribute('y',evt.clientY+27+16);
   tt1.setAttribute('visibility','visible');
   if(tt2.firstChild.data === '')
   {
      ttb.setAttribute('height', 18);
   }
   else
   {
      tt2.setAttribute('visibility','visible');
      ttb.setAttribute('height', 33);
   }

   ttb.setAttribute('width',length+6);
   ttb.setAttribute('y',evt.clientY+14);
   ttb.setAttribute('visibility','visible');
}
function tt_off(evt)
{
   tt1.setAttribute('visibility','hidden');
   tt2.setAttribute('visibility','hidden');
   ttb.setAttribute('visibility','hidden');
}
function progress_colour(col)
{
   var progc = svg_doc.getElementById('progc');
   if(progc) progc.style.fill = col;
}
