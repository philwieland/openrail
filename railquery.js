/*
    Copyright (C) 2015, 2017 Phil Wieland

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

var url_base = "/rail/query/";
var liverail_url_base = "/rail/liverail/";

function search_onclick()
{
   result = liverail_url_base + "full" + '/' + document.getElementById("search_loc").value + '/' + document.getElementById("search_date").value;
   window.location = result;
}

function freight_onclick()
{
   var result = liverail_url_base + "frt" + '/' + document.getElementById("search_loc").value + '/' + document.getElementById("search_date").value;
   window.location = result;
}

function summary_onclick()
{
   result = liverail_url_base + "sum" + '/' + document.getElementById("search_loc").value + '/' + document.getElementById("search_date").value;
   window.location = result;
}

function depart_onclick()
{
   result = liverail_url_base + "dep" + '/' + document.getElementById("search_loc").value + '/' + document.getElementById("search_date").value;
   window.location = result;
}

function menu_onclick()
{
   result = url_base;
   window.location = result;
}

function A_onclick()
{
   result = liverail_url_base + 'train/' + document.getElementById("A-id").value + '/' + document.getElementById("date").value;
   window.location = result;
}

function B_onclick()
{
   result = url_base + 'B/' + document.getElementById("B-u").value;
   if(document.getElementById("B-w").checked)
   {
      result += '/W';
   }
   window.location = result;
}

function C_onclick()
{
   result = url_base + 'C/' + document.getElementById("C-h").value;
   if(document.getElementById("C-w").checked)
   {
      result += '/W';
   }

   window.location = result;
}

function D_onclick()
{
   result = url_base + 'D/' + document.getElementById("D-s").value;
   if(document.getElementById("D-w").checked)
   {
      result += '/W';
   }

   window.location = result;
}

function E_onclick()
{
   result = url_base + 'E/' + document.getElementById("E-v").value;
   if(document.getElementById("E-w").checked)
   {
      result += '/W';
   }
   window.location = result;
}

function F_onclick()
{
   result = url_base + 'F/' + 
      document.getElementById("F-id").value + '/' +
      document.getElementById("F-l").value;
   window.location = result;
}

function startup()
{
}

