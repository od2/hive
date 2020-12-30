local function _envoy_on_request(request)
  -- Extract OAuth 2.0 authorization code
  local auth_header = request:headers():get("authorization")
  local token = string.gsub(auth_header, "Bearer ", "")
  request:headers():remove("authorization")
  -- Lookup GitHub identity
  local gh_headers, gh_body = request:httpCall(
    "github-api-loopback",
    {
      [":method"] = "GET", [":authority"] = "api.github.com", [":path"] = "/user",
      ["authorization"] = "token " .. token,
      ["user-agent"] = "envoy od2-github-auth"
    },
    nil, 5000)
  -- Set upstream header
  local gh_status = gh_headers[":status"]
  if gh_status ~= "200" then
    error("GitHub get user status: " .. gh_status)
  end
  request:logInfo("GitHub token: " .. gh_body)
  local gh_parse = require "od2.github_identity"
  local identity, err = gh_parse.extract_login(gh_body)
  if err then
    error("GitHub identity parse: " .. err)
  end
  request:headers():replace("x-od2-identity", identity)
end

function envoy_on_request(request)
  local status, err = pcall(_envoy_on_request, request)
  if err then
    request:logErr(err)
    request:respond({ [":status"] = "500" })
  end
end
