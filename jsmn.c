#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "jsmn.h"
#include "misc.h"

/**
 * Allocates a fresh unused token from the token pull.
 */
static jsmntok_t *jsmn_alloc_token(jsmn_parser *parser, 
		jsmntok_t *tokens, size_t num_tokens) 
{
   jsmntok_t *tok;
   if (parser->toknext >= num_tokens - 1)
   {
      tokens[num_tokens - 1].start = tokens[num_tokens - 1].end = -1;
      return NULL;
   }
   tok = &tokens[parser->toknext++];
   tok->start = tok->end = -1;
   tok->size = 0;
   return tok;
}

/**
 * Fills token type and boundaries.
 */
static void jsmn_fill_token(jsmntok_t *token, jsmntype_t type, 
                            int start, int end) 
{
   token->type = type;
   token->start = start;
   token->end = end;
   token->size = 0;
}

/**
 * Fills next available token with JSON primitive.
 */
static jsmnerr_t jsmn_parse_primitive(jsmn_parser *parser, const char *js,
		jsmntok_t *tokens, size_t num_tokens) 
{
   jsmntok_t *token;
   int start;
   
   start = parser->pos;
   
   for (; js[parser->pos] != '\0'; parser->pos++) 
   {
      switch (js[parser->pos]) 
      {
      case ':':
      case '\t' : case '\r' : case '\n' : case ' ' :
      case ','  : case ']'  : case '}' :
         goto found;
      }
      if (js[parser->pos] < 32 || js[parser->pos] >= 127) {
         parser->pos = start;
         return JSMN_ERROR_INVAL;
      }
   }

 found:
   token = jsmn_alloc_token(parser, tokens, num_tokens);
   if (token == NULL) 
   {
      parser->pos = start;
      return JSMN_ERROR_NOMEM;
   }
   jsmn_fill_token(token, JSMN_PRIMITIVE, start, parser->pos);
   parser->pos--;
   return JSMN_SUCCESS;
}

/**
 * Filsl next token with JSON string.
 */
static jsmnerr_t jsmn_parse_string(jsmn_parser *parser, const char *js,
		jsmntok_t *tokens, size_t num_tokens) 
{
   jsmntok_t *token;

   int start = parser->pos;

   parser->pos++;

   /* Skip starting quote */
   for (; js[parser->pos] != '\0'; parser->pos++) 
   {
      char c = js[parser->pos];

      /* Quote: end of string */
      if (c == '\"') 
      {
         token = jsmn_alloc_token(parser, tokens, num_tokens);
         if (token == NULL) 
         {
            parser->pos = start;
            return JSMN_ERROR_NOMEM;
         }
         if(js[parser->pos + 1] == ':')
         {
            jsmn_fill_token(token, JSMN_NAME, start+1, parser->pos);
         }
         else
         {
            jsmn_fill_token(token, JSMN_STRING, start+1, parser->pos);
         }
         return JSMN_SUCCESS;
      }

      /* Backslash: Quoted symbol expected */
      if (c == '\\') 
      {
         parser->pos++;
         switch (js[parser->pos]) 
         {
            /* Allowed escaped symbols */
         case '\"': case '/' : case '\\' : case 'b' :
         case 'f' : case 'r' : case 'n'  : case 't' :
            break;
            /* Allows escaped symbol \uXXXX */
         case 'u':
            /* TODO */
            break;
            /* Unexpected symbol */
         default:
            parser->pos = start;
            return JSMN_ERROR_INVAL;
         }
      }
   }
   parser->pos = start;
   return JSMN_ERROR_PART;
}

/**
 * Parse JSON string and fill tokens.
 */
jsmnerr_t jsmn_parse(jsmn_parser *parser, const char *js, jsmntok_t *tokens, 
		unsigned int num_tokens) 
{
   jsmnerr_t r;
   int i;
   jsmntok_t *token;
   
   for (; js[parser->pos] != '\0'; parser->pos++) 
   {
      char c;
      jsmntype_t type;

      c = js[parser->pos];
      switch (c) 
      {
      case '{': case '[':
         token = jsmn_alloc_token(parser, tokens, num_tokens);
         if (token == NULL)
            return JSMN_ERROR_NOMEM;
         if (parser->toksuper != -1) {
            tokens[parser->toksuper].size++;
         }
         token->type = (c == '{' ? JSMN_OBJECT : JSMN_ARRAY);
         token->start = parser->pos;
         parser->toksuper = parser->toknext - 1;
         break;

      case '}': case ']':
         type = (c == '}' ? JSMN_OBJECT : JSMN_ARRAY);
         for (i = parser->toknext - 1; i >= 0; i--) {
            token = &tokens[i];
            if (token->start != -1 && token->end == -1) {
               if (token->type != type) {
                  return JSMN_ERROR_INVAL;
               }
               parser->toksuper = -1;
               token->end = parser->pos + 1;
               break;
            }
         }
         /* Error if unmatched closing bracket */
         if (i == -1) return JSMN_ERROR_INVAL;
         for (; i >= 0; i--) {
            token = &tokens[i];
            if (token->start != -1 && token->end == -1) {
               parser->toksuper = i;
               break;
            }
         }
         break;
      case '\"':
         r = jsmn_parse_string(parser, js, tokens, num_tokens);
         if (r < 0) return r;
         if (parser->toksuper != -1)
            tokens[parser->toksuper].size++;
         break;
      case '\t' : case '\r' : case '\n' : case ':' : case ',': case ' ': 
         break;

      default:
         r = jsmn_parse_primitive(parser, js, tokens, num_tokens);
         if (r < 0) return r;
         if (parser->toksuper != -1)
            tokens[parser->toksuper].size++;
         break;
         
      }
   }
   
   for (i = parser->toknext - 1; i >= 0; i--) {
      /* Unmatched opened object or array */
      if (tokens[i].start != -1 && tokens[i].end == -1) {
         return JSMN_ERROR_PART;
      }
   }

   // Append an "END" token
   if(parser->toknext < num_tokens)
   {
      tokens[parser->toknext].start = tokens[parser->toknext].end = -1;
   }
   return JSMN_SUCCESS;
}

/**
 * Creates a new parser based over a given  buffer with an array of tokens 
 * available.
 */
void jsmn_init(jsmn_parser *parser) {
	parser->pos = 0;
	parser->toknext = 0;
	parser->toksuper = -1;
}

word jsmn_find_name_token(const char * string, const jsmntok_t * tokens, const word object_index, const char * search)
{
   // Search the object at token number object_index for a name of value search, and return its token number or 0 for not found.
   char zs[2048];

   word i;
   for(i = object_index + 1; tokens[i].start >= 0 && tokens[i].start < tokens[object_index].end; i++)
   {
      if(tokens[i].type == JSMN_NAME)
      {
         jsmn_extract_token(string, tokens, i, zs, sizeof(zs));
         if(!strcasecmp(zs, search))
         {
            // Hit
            return i;
         }
      }
   }

   //sprintf(zs, "jsmn_find_name_token(\"%s\") Miss.", search);
   //_log(MAJOR, zs);

   return 0;
}

void jsmn_extract_token(const char * string, const jsmntok_t * tokens, const word index, char * result, const size_t max_length)
{
   // Note:  Doesn't check the token type.  Truncates to fit if necessary.
   size_t length = tokens[index].end-tokens[index].start;
   if (length > max_length - 1) length = max_length - 1;

   strncpy(result, string + tokens[index].start, length);
   result[length] = '\0';

   if(strcmp(result, "null") == 0) result[0] = '\0';
}

void jsmn_find_extract_token(const char * string, const jsmntok_t * tokens, const word object_index, const char * search, char * result, const size_t max_length)
{
   // Search the object at token number object_index for a name of value search, and return the value of the next token.
   size_t index = jsmn_find_name_token(string, tokens, object_index, search);
   if(index > 0 && tokens[index + 1].start >= 0)
   {
      jsmn_extract_token(string, tokens, index + 1, result, max_length);
   }
   else
   {
      result[0] = '\0';
   }
}
      
