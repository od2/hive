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
void jv_free(jv);

jv_kind jv_get_kind(jv);
int jv_string_length_bytes(jv);
const char* jv_string_value(jv);

/* JQ processing */
struct jq_state;
typedef struct jq_state jq_state;

jq_state *jq_init(void);
void jq_teardown(jq_state **);

int jq_compile(jq_state *, const char *);

void jq_start(jq_state *, jv value, int);
jv jq_next(jq_state *);
]]

local jq = ffi.load("jq.so.1")

local _M = {}

function _M.extract_login (json)
  -- Create new jq context
  local ctx = jq.jq_init()
  assert(ctx ~= 0)
  -- Compile jq filter
  local filter = jq.jq_compile(ctx, ".login")
  -- Parse input
  local parsed = jq.jv_parse_sized(json, #json)
  local parsed_kind = jq.jv_get_kind(parsed)
  if parsed_kind ~= jq.JV_KIND_OBJECT then
    return nil, "invalid JSON"
  end
  -- Process input through filter
  jq.jq_start(ctx, parsed, 0)
  local result = jq.jq_next(ctx)
  local result_kind = jq.jv_get_kind(result)
  if result_kind ~= jq.JV_KIND_STRING then
    return nil, "invalid result"
  end
  -- Read input to string
  local result_len = jq.jv_string_length_bytes(result) 
  local result_chars = jq.jv_string_value(result)
  local result_str = ffi.string(result_chars, result_len)
  -- Teardown jq context
  local ctx_arr = ffi.new("jq_state *[1]")
  ctx_arr[0] = ctx
  jq.jq_teardown(ctx_arr)
  return result_str, nil
end

return _M
