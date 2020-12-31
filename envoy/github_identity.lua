local ffi = require "ffi"
ffi.cdef[[
/* JV parsing */
struct jv_refcnt;
typedef struct {
  unsigned char kind_flags;
  unsigned char pad_;
  unsigned short offset;
  int size;
  union {
    struct jv_refcnt* ptr;
    double number;
  } u;
} jv;
typedef enum {
  JV_KIND_INVALID,
  JV_KIND_NULL,
  JV_KIND_FALSE,
  JV_KIND_TRUE,
  JV_KIND_NUMBER,
  JV_KIND_STRING,
  JV_KIND_ARRAY,
  JV_KIND_OBJECT
} jv_kind;

jv jv_parse_sized(const char* string, int length);
jv jv_string_sized(const char*, int);
jv jv_copy(jv);
void jv_free(jv);

jv_kind jv_get_kind(jv);

int jv_string_length_bytes(jv);
const char* jv_string_value(jv);

double jv_number_value(jv);

jv jv_object_get(jv object, jv key);
]]

local jq = ffi.load("jq")

local jv_meta = {}
local jv_class = {}
function jv_meta.__index (obj, key)
  return jv_class[key]
end

local function jv_wrap(val)
  local val_t = {val}
  setmetatable(val_t, jv_meta)
  local val = ffi.gc(val, function (obj) jq.jv_free(obj) end)
  return val_t
end

function jv_class.kind(obj)
  return jq.jv_get_kind(obj[1])
end
function jv_class.string(obj)
  if obj:kind() ~= jq.JV_KIND_STRING then
    return nil
  end
  local obj_copy = jq.jv_copy(obj[1])
  local str_len = jq.jv_string_length_bytes(obj_copy)
  local str_val = jq.jv_string_value(obj_copy)
  return ffi.string(str_val, str_len)
end
function jv_class.object_get(obj, key)
  if obj:kind() ~= jq.JV_KIND_OBJECT then
    return nil
  end
  local key_jv = jq.jv_string_sized(key, #key)
  local obj_copy = jq.jv_copy(obj[1])
  local val = jq.jv_object_get(obj_copy, key_jv)
  return jv_wrap(val)
end
function jv_class.number(obj)
  if obj:kind() ~= jq.JV_KIND_NUMBER then
    return nil
  end
  local obj_copy = jq.jv_copy(obj[1])
  return jq.jv_number_value(obj_copy)
end

local function jv_parse (json)
  local val = jq.jv_parse_sized(json, #json)
  return jv_wrap(val)
end

local function extract_login (json)
  local parsed = jv_parse(json)
  if parsed:kind() ~= jq.JV_KIND_OBJECT then
    return nil, "invalid JSON"
  end
  local result = {
    login = parsed:object_get("login"):string(),
    id    = parsed:object_get("id"):number(),
  }
  return result, nil
end
