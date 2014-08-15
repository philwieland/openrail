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

var url_base = "/rail/livesig/";
var refresh_tick = 8192; /* ms between ticks */
var updating_timeout = 0;
var got_handle = 32767;
var elapsed;
var req;
var req_cache = null;
var req_cache_k;
var req_cache_timout = 0;
var feed_failed = 0;

var svg_doc;

var SLOTS = 12;
var displayed = new Array(SLOTS);
var cache_k   = new Array(SLOTS);
var cache_v   = new Array(SLOTS);
var cache_s   = new Array(SLOTS);

function startup()
{
   setInterval('tick()', refresh_tick);
   refresh_count = 0;
   updating_timeout = 0;
   req_cache_timeout = 0;
}

function tick()
{
   if(req_cache_timeout)
   {
      if(req_cache_timeout++ > 10)
      {
         // Cache update has timed out
         if(req_cache) req_cache.abort();
         req_cache = null;
         req_cache_timeout = 0;
         update_panel();
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
      elapsed = new Date().getTime();

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
      req.open('GET', url_base + 'U/' + got_handle, true);
      req.send(null);
   }
}

function process_updates(text)
{
   var results = text.split("\n");

   var index = 1; // Index of first data line.

   if(!document.getElementById('diagram').contentDocument)
   {
      document.getElementById('bottom-line').innerHTML = " Sorry, does not work in this browser with your current settings.";
      updating_timeout = 0;
      return;
   }
      
   svg_doc = document.getElementById('diagram').contentDocument;

   got_handle = results[index - 1];
   while(results.length > index && results[index].length > 2 && (results[index].substr(0, 1) === 'b' || results[index].substr(0, 1) === 's'))
   {
      var parts = results[index++].split('|');
      if(parts.length > 1)
      {
         if(parts[0].substr(0,1) == 'b')
         {
            update_berth(parts[0], parts[1]);
         }
         else
         {
            update_signal(parts[0], parts[1]);
         }
      }
   }
   
   update_panel();
   var caption = results[index].split('|');
   elapsed = new Date().getTime() - elapsed;
   svg_doc.getElementById('caption').textContent = 'Updated to ' + caption[1] + ' by ' + caption[2] + ' ' + caption[3] + ' at ' + caption[4] + '  Elapsed time ' + elapsed + ' ms. ';
   if(caption[0] > 0)
   {
      feed_failed = 10;
   }
   
   if(feed_failed)
   {
      feed_failed--;
      svg_doc.getElementById('caption').style.stroke = 'red';
      svg_doc.getElementById('caption').textContent += "  Data feed failed.";
   }
   else
   {
      svg_doc.getElementById('caption').style.stroke = 'black';
   }
   updating_timeout = 0;
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
      if(v != '')
      {
         for(i=0; i < SLOTS; i++)
         {
            if(!displayed[i] || displayed[i] == '')
            {
               displayed[i] = v;
               i = SLOTS;
            }
         }
      }
      svg_doc.getElementById(k).textContent = v;
   }
}

function update_signal(k, v)
{
   var i;
   if(svg_doc.getElementById(k))
   {
      if(k.substr(1, 1) === '5' || k.substr(1, 1) === '6')
      {
         svg_doc.getElementById(k).style.stroke = ((v === '1')?'black':'#dddddd');
         //svg_doc.getElementById(k).style.stroke = ((v === '1')?'blue':'#bbbbbb');
      }
      else
      {
         svg_doc.getElementById(k).style.fill = ((v === '1')?'#00ff00':'red');
      }
   }
}

function update_panel()
{
   var i;
   var j;
   displayed.sort();

   j = 0;
   for(i=0; i < SLOTS; i++)
   {
      if(displayed[i] && displayed[i] != '')
      {
         svg_doc.getElementById('info' + j++).textContent = displayed[i] + ' ' + cached(displayed[i]);
      }
   }
   for(;j < SLOTS; j++)
   {
      svg_doc.getElementById('info' + j).textContent = '';
   }

}

function cached(k)
{
   var i;
   var now =  new Date().getTime();
   for(i = 0; i < SLOTS; i++)
   {
      if(cache_k[i] && cache_k[i] == k)
      {
         cache_s[i] = now;
         return cache_v[i];
      }
   }

   if(!req_cache)
   {
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
               update_panel();
            }
            else
            {
               // Failed.  No action, try again on next tick.
               req_cache = null;
            }
         }
      }
      req_cache.open('GET', url_base + 'Q/' + k, true);
      req_cache.send(null);
      return '';
   }
   return '';
}

function update_cache(k, v)
{
   // Called on async response from cache query
   var i;
   var now = new Date().getTime();
   var oldest = now;
   var chosen;
   var results = v.split("\n");

   for(i = 0; i < SLOTS; i++)
   {
      if(!cache_k[i] || cache_k[i] == '')
      {
         chosen = i;
         i = SLOTS;
      }
      else if(cache_s[i] && cache_s[i] < oldest)
      {
         oldest = cache_s[i];
         chosen = i;
      }
   }
   cache_k[chosen] = k;
   cache_v[chosen] = results[0];
   cache_s[chosen] = now;
}
