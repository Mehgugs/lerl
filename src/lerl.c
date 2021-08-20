#include <lua.h>
#include <luaconf.h>
#include <lauxlib.h>
#include <encoder.h>
#include <sysdep.h>
#include <limits.h>
#include <stdbool.h>
#include <zlib.h>
#include <inttypes.h>

#define lerl_encoder_type "lerl_encoder"
#define lerl_decoder_type "lerl_decoder"

#define lerl_array_mt "lerl_decoded_array"
#define lerl_map_mt "lerl_decoded_map"

#define DEFAULT_RECURSE_LIMIT 256
#define INITIAL_BUFFER_SIZE (1024 * 1024)

#define check_ret(n) if ((ret) != 0) { \
    e->ret = (ret); \
    lua_warning(L, "lerl_encode.pack: Issue when packing in " n " return non-zero.", false); \
    return ret; \
}

#define take_ret() if ((ret) != 0) { \
    e->ret = (ret); \
    return ret; \
}

typedef struct {
    erlpack_buffer pk;
    int ret;
} lerl_encoder;

static lerl_encoder* lerl_get_encoder(lua_State* L, int at) {
    return luaL_checkudata(L, at, lerl_encoder_type);
}

static int lerl_new_encoder2(lua_State* L, bool skip_version) {

    lerl_encoder* the_encoder = lua_newuserdata(L, sizeof(lerl_encoder));

    the_encoder->pk.buf = (char*)malloc(INITIAL_BUFFER_SIZE);
    the_encoder->pk.allocated_size = INITIAL_BUFFER_SIZE;
    the_encoder->pk.length = 0;
    the_encoder->ret = 0;
    if (!skip_version)
        the_encoder->ret = erlpack_append_version(&the_encoder->pk);

    if (the_encoder->ret != 0)
        return luaL_error(L, "lerl_encoder.new: Unable to set version header.");

    luaL_getmetatable(L, lerl_encoder_type);
    lua_setmetatable(L, -2);
    return 1;
}

static int lerl_new_encoder(lua_State* L) {
    return lerl_new_encoder2(L, false);
}

static int lerl_encoder_gc(lua_State* L) {
    lerl_encoder* e = lerl_get_encoder(L, 1);

    if (e->pk.buf != NULL) {
        free(e->pk.buf);
    }

    e->pk.buf = NULL;
    e->pk.allocated_size = 0;
    e->pk.length = 0;
    return 0;
}

static int lerl_pack_at(lua_State* L, int encoder_at, int object_at, int limit) {
    if (limit <= 0)
        return luaL_error(L, "lerl_encoder:pack Maximum pack depth reached!");

    lerl_encoder* e = lerl_get_encoder(L, encoder_at);

    if (e->ret != 0)
        return luaL_error(L, "lerl_encoder:pack Encoder buffer is in a bad state.");

    int the_type = lua_type(L, object_at);
    int ret;
    switch (the_type) {
        case LUA_TNIL:;
            ret = erlpack_append_nil(&e->pk);
            check_ret("pack nil")
            break;
        case LUA_TBOOLEAN:;
            ret = lua_toboolean(L, object_at) ? erlpack_append_true(&e->pk) : erlpack_append_false(&e->pk);
            check_ret("pack boolean")
            break;
        case LUA_TNUMBER:
            if (lua_isinteger(L, object_at)) {
                lua_Integer I = lua_tointeger(L, object_at);

                if (0 <= I && I <= 255) {
                    ret = erlpack_append_small_integer(&e->pk, (unsigned char)I);
                    check_ret("pack small integer")
                } else {
#if LUA_INT_TYPE == LUA_INT_LONGLONG
                    ret = erlpack_append_long_long(&e->pk, I);
                    check_ret("pack long long integer")
#elif LUA_INT_TYPE == LUA_INT_LONG
                    ret = erlpack_append_integer(&e->pk, I);
                    check_ret("pack long integer")
#elif LUA_INT_TYPE == LUA_INT_INT
                    ret = erlpack_append_integer(&e->pk, I);
                    check_ret("pack integer")
#endif
                }
            } else {
                lua_Number N = lua_tonumber(L, object_at);
                ret = erlpack_append_double(&e->pk, (double)N);
                check_ret("pack double")
            }
        break;

        case LUA_TSTRING:;
            size_t len;
            const char *str = lua_tolstring(L, object_at, &len);
            ret = erlpack_append_binary(&e->pk, str, len);
            check_ret("pack string")
            break;
        case LUA_TTABLE:
            if (luaL_getmetafield(L, object_at, "__lerl_type") == LUA_TSTRING) {
                size_t flen;
                const char* ttype = lua_tolstring(L, -1, &flen);

                if (flen == 5 && strncmp(ttype, "array", 5) == 0) {
                    lua_pop(L, 1);
                    size_t count = 0;

                    ret = erlpack_append_list_header(&e->pk, 0);
                    size_t destination = e->pk.length - 4;

                    check_ret("pack list header")

                    for (;;) {
                        count = count + 1;

                        if (lua_geti(L, object_at, count) == LUA_TNIL) {
                            lua_pop(L, 1);
                            break;
                        }

                        ret = lerl_pack_at(L, 1, lua_gettop(L), limit - 1);

                        lua_pop(L, 1);
                        take_ret()
                    }

                    count = count - 1;

                    size_t end = e->pk.length;
                    e->pk.length = destination;
                    char count_buf[4] = {0};
                    _erlpack_store32(count_buf, count);
                    ret = erlpack_buffer_write(&e->pk, (const char *)count_buf, 4);
                    e->pk.length = end;

                    check_ret("upsert list length")

                    ret = erlpack_append_nil_ext(&e->pk);

                    check_ret("pack nil tail")

                } else if (flen == 3 && strcmp(ttype, "map") == 0) {
                    lua_pop(L, 1);
                    size_t count = 0;


                    ret = erlpack_append_map_header(&e->pk, count);

                    check_ret("pack map header")

                    size_t destination = e->pk.length - 4;
                    lua_pushnil(L);

                    while (lua_next(L, object_at) != 0) {
                        count = count + 1;
                        if (count > INT32_MAX)
                            return luaL_error(L, "lerl_encoder.pack: lerl.map has too many key-value properties!");

                        int top = lua_gettop(L);

                        ret = lerl_pack_at(L, 1, top - 1, limit - 1);
                        take_ret()

                        ret = lerl_pack_at(L, 1, top, limit - 1);
                        take_ret()

                        lua_pop(L, 1);
                    }

                    size_t end = e->pk.length;
                    e->pk.length = destination;
                    char count_buf[4] = {0};
                    _erlpack_store32(count_buf, count);
                    ret = erlpack_buffer_write(&e->pk, (const char *)count_buf, 4);
                    e->pk.length = end;

                    check_ret("upsert map length")

                } else if (flen == 4 && strncmp(ttype, "user", 4) == 0) {
                    lua_pop(L, 1);
                    if (luaL_getmetafield(L, object_at, "__lerl_user") != LUA_TNIL) {
                        lua_pushvalue(L, object_at);
                        lua_call(L, 1, 1);
                        return lerl_pack_at(L, encoder_at, lua_gettop(L), limit - 1);
                    }
                } else {
                    return luaL_error(L, "lerl_encoder.pack: Unsure what to do with a table with a strange lerl_type set.");
                }
            } else {
                return luaL_error(L, "lerl_encoder.pack: Unsure what to do with a table with no lerl_type set.");
            }
            break;

        default:
            return luaL_error(L, "lerl_encoder.pack: You cannot pack a %s.", lua_typename(L, the_type));
    }
    return 0;
}

static int lerl_release(lua_State* L) {
    lerl_encoder* e = lerl_get_encoder(L, 1);
    if (e->pk.length == 0 || e->pk.buf == NULL) {
        lua_pushliteral(L, "");
    } else {
        lua_pushlstring(L, e->pk.buf, e->pk.length);
        e->pk.length = 0;
        e->ret = erlpack_append_version(&e->pk);
        if (e->ret != 0)
            lua_warning(L, "lerl_encoder.release: Issue re-initializing buffer.", 0);
    }
    return 1;
}

static int lerl_pack(lua_State* L) {
   luaL_argcheck(L, !lua_isnone(L, 2), 2, "You must pass nil explicitly to encode nil.");

   lerl_pack_at(L, 1, 2, DEFAULT_RECURSE_LIMIT);
   lua_settop(L, 1);
   return 1;
}

static int lerl_pack_all(lua_State* L) {
    lerl_encoder* e = lerl_get_encoder(L, 1);
    int slots = lua_gettop(L);
    int count = 1;
    while (count < slots) {
        count = count + 1;
        lerl_pack_at(L, 1, count, DEFAULT_RECURSE_LIMIT);
    }
    lua_settop(L, 1);
    return 1;
}

static luaL_Reg encoder_metamethods[] = {
    {"__gc", lerl_encoder_gc},
    {NULL, NULL}
};

static luaL_Reg encoder_methods[] = {
    {"pack", lerl_pack},
    {"pack_all", lerl_pack_all},
    {"release", lerl_release},
    {NULL, NULL}
};

static int lerl_encoder_init(lua_State* L) {
    luaL_newmetatable(L, lerl_encoder_type);
    luaL_setfuncs(L, encoder_metamethods, 0);
    lua_pushliteral(L, "__index");
    lua_createtable(L, 0, 2);
    luaL_setfuncs(L, encoder_methods, 0);
    lua_settable(L, -3);
    lua_pop(L, 1);
    return 0;
}


typedef struct {
    char* data;
    size_t size;
    bool invalid;
    int offset;
    int empty_ref;
} lerl_decoder;

static int lerl_unpack(lua_State* L);

static lerl_decoder* lerl_get_decoder(lua_State* L, int at) {
    return luaL_checkudata(L, at, lerl_decoder_type);
}

static uint8_t lerl_read8_out(lua_State* L) {
    lerl_decoder* the_decoder = lerl_get_decoder(L, 1);
    int offset = the_decoder->offset;
    char* data = the_decoder->data;
    if (offset + sizeof(uint8_t) > the_decoder->size)
        return luaL_error(L, "lerl_decoder.read8: Reading passes the end of the buffer.");

    const uint8_t val = data[offset];
    the_decoder->offset = offset + sizeof(uint8_t);
    return val;
}

static int lerl_new_decoder(lua_State* L) {
    size_t size;
    const char* buf = luaL_checklstring(L, 1, &size);
    int empty_ref;
    if (lua_gettop(L) >= 2 && !lua_isnoneornil(L, 2)) {
        lua_settop(L, 2);
        lua_pushvalue(L, 2);
        empty_ref = luaL_ref(L, LUA_REGISTRYINDEX);
    } else {
        lua_getfield(L, LUA_REGISTRYINDEX, "lerl_empty");
        empty_ref = lua_tointeger(L, -1);
        lua_pop(L, 1);
    }

    lerl_decoder* the_decoder = lua_newuserdata(L, sizeof(lerl_decoder));
    char* data = malloc(size * sizeof(char));

    if (data == NULL)
        return luaL_error(L, "lerl_decoder.new: Failed to allocate buffer!");

    memcpy(data, buf, size);
    lua_remove(L, 1);

    the_decoder->data = data;
    the_decoder->size = size;
    the_decoder->offset = 0;
    the_decoder->empty_ref = empty_ref;
    the_decoder->invalid = 0;

    luaL_getmetatable(L, lerl_decoder_type);
    lua_setmetatable(L, -2);
    int ver = lerl_read8_out(L);
    if(ver != FORMAT_VERSION)
        return luaL_error(L, "lerl_decoder.new: Version mismatch!");

    return 1;
}

static int lerl_empty_decoder(lua_State* L) {

    int empty_ref;
    if (lua_gettop(L) >= 1 && !lua_isnoneornil(L, 1)) {
        lua_settop(L, 1);
        lua_pushvalue(L, 1);
        empty_ref = luaL_ref(L, LUA_REGISTRYINDEX);
    } else {
        lua_getfield(L, LUA_REGISTRYINDEX, "lerl_empty");
        empty_ref = lua_tointeger(L, -1);
        lua_pop(L, 1);
    }

    lerl_decoder* the_decoder = lua_newuserdata(L, sizeof(lerl_decoder));
    the_decoder->data = NULL;
    the_decoder->size = 0;
    the_decoder->offset = 0;
    the_decoder->empty_ref = empty_ref;
    the_decoder->invalid = true;

    luaL_getmetatable(L, lerl_decoder_type);
    lua_setmetatable(L, -2);

    return true;
}

static int lerl_reset_decoder(lua_State* L) {
    lerl_decoder* the_decoder = lerl_get_decoder(L, 1);
    size_t size;
    const char* buf = luaL_checklstring(L, 2, &size);

    char* data = malloc(size * sizeof(char));
    memcpy(data, buf, size);
    lua_pop(L, 1);

    the_decoder->data = data;
    the_decoder->size = size;
    the_decoder->offset = 0;
    the_decoder->invalid = 0;

    int ver = lerl_read8_out(L);
    if(ver != FORMAT_VERSION)
        return luaL_error(L, "lerl_decoder.reset: Version mismatch!");

    return 1;
}

static int lerl_read8(lua_State* L) {
    lua_pushinteger(L, lerl_read8_out(L));
    return 1;
}

static uint16_t lerl_read16_out(lua_State* L) {
    lerl_decoder* the_decoder = lerl_get_decoder(L, 1);
    int offset = the_decoder->offset;
    char* data = the_decoder->data;
    if (offset + sizeof(uint16_t) > the_decoder->size)
        return luaL_error(L, "lerl_decoder.read16: Reading passes the end of the buffer.");

    const uint16_t val = _erlpack_be16(*(const uint16_t*)(data + offset));
    the_decoder->offset = offset + sizeof(uint16_t);
    return val;
}

static int lerl_read16(lua_State* L) {
    lua_pushinteger(L, lerl_read16_out(L));
    return 1;
}

static uint32_t lerl_read32_out(lua_State* L) {
    lerl_decoder* the_decoder = lerl_get_decoder(L, 1);
    int offset = the_decoder->offset;
    char* data = the_decoder->data;
    if (offset + sizeof(uint32_t) > the_decoder->size)
        return luaL_error(L, "lerl_decoder.read32: Reading passes the end of the buffer.");

    const uint32_t val = _erlpack_be32(*(const uint32_t*)(data + offset));
    the_decoder->offset = offset + sizeof(uint32_t);
    return val;
}

static int lerl_read32(lua_State* L) {
    lua_pushinteger(L, lerl_read32_out(L));
    return 1;
}

static uint64_t lerl_read64_out(lua_State* L) {
    lerl_decoder* the_decoder = lerl_get_decoder(L, 1);
    int offset = the_decoder->offset;
    char* data = the_decoder->data;
    if (offset + sizeof(uint64_t) > the_decoder->size)
        return luaL_error(L, "lerl_decoder.read64: Reading passes the end of the buffer.");

    const uint64_t val = _erlpack_be64(*(const uint64_t*)(data + offset));
    the_decoder->offset = offset + sizeof(uint64_t);
    return val;
}

static int lerl_read64(lua_State* L) {
    lua_pushinteger(L, (lua_Integer)lerl_read64_out(L));
    return 1;
}

static int lerl_decodeSmallInteger(lua_State* L) {
    lerl_read8(L);
    return 1;
}

static int lerl_decodeInteger(lua_State* L) {
    lerl_read32(L);
    return 1;
}

static int lerl_decodeSequential(lua_State* L, uint32_t length) {
    lerl_decoder* the_decoder = lerl_get_decoder(L, 1);
    lua_createtable(L, length, 0);
    for (lua_Integer i = 1; i <= length; i++) {
        lerl_unpack(L);
        if (the_decoder->invalid) {
            return 0;
        }
        lua_seti(L, -2, i);
    }
    return 1;
}

static int lerl_decodeList(lua_State* L) {
    lerl_decodeSequential(L, lerl_read32_out(L));
    luaL_getmetatable(L, lerl_array_mt);
    lua_setmetatable(L, -2);
    uint8_t tailMarker = lerl_read8_out(L);
    if (tailMarker != NIL_EXT)
        return luaL_error(L, "lerl_decoder.decodeList: List doesn't end with a tail marker.");
    return 1;
}

static int lerl_decodeNil(lua_State* L) {
    lua_createtable(L, 0, 0);
    return 1;
}


static int lerl_decodeMap(lua_State* L) {
    lerl_decoder* the_decoder = lerl_get_decoder(L, 1);
    int length = lerl_read32_out(L);

    lua_createtable(L, 0, length);
    luaL_getmetatable(L, lerl_map_mt);
    lua_setmetatable(L, -2);

    for (int i = 0; i < length; ++i) {
        lerl_unpack(L);
        lerl_unpack(L);
        if (the_decoder->invalid){
            return 0;
        }
        lua_settable(L, -3);
    }
    return 1;
}

const char* lerl_readString(lua_State* L, uint32_t length) {
    lerl_decoder* the_decoder = lerl_get_decoder(L, 1);
    int offset = the_decoder->offset;
    char* data = the_decoder->data;
    if (offset + length > the_decoder->size){
        luaL_error(L, "lerl_decoder.readString: Reading passes the end of the buffer.");
        return NULL;
    }

    the_decoder->offset = offset + length;
    return (const char*)(data + offset);
}

static int lerl_processAtom(lua_State* L, const char* atom, uint16_t len) {
    lerl_decoder* the_decoder = lerl_get_decoder(L, 1);
    if (atom == NULL){
        return 0;
    }

    if (len >= 3 && len <= 5) {
        if (len == 3 && strncmp(atom, "nil", 3) == 0 ){
            lua_pushnil(L);
        } else if (len == 4 && strncmp(atom, "null", 4) == 0 ){
            lua_rawgeti(L, LUA_REGISTRYINDEX, the_decoder->empty_ref);
        } else if (len == 4 && strncmp(atom, "true", 4) == 0 ){
            lua_pushboolean(L, 1);
        } else if (len == 5 && strncmp(atom, "false", 5) == 0 ){
            lua_pushboolean(L, 0);
        }
    } else
        lua_pushlstring(L, atom, len);
    return 1;
}

static int lerl_decodeAtom(lua_State* L) {
    uint16_t len = lerl_read16_out(L);
    const char* atom = lerl_readString(L, len);
    lerl_processAtom(L, atom, len);
    return 1;
}

static int lerl_decodeSmallAtom(lua_State* L) {
    uint8_t len = lerl_read8_out(L);
    const char* atom = lerl_readString(L, len);
    lerl_processAtom(L, atom, len);
    return 1;
}

static int lerl_decodeFloat(lua_State* L) {
    const char* floatStr = lerl_readString(L, 31);
    if (floatStr == NULL){
        return 0;
    }

    lua_Number number;
    char nullterminated[32] = {0};

    memcpy(nullterminated, floatStr, 31);

    int count = sscanf(nullterminated, "%lf", &number);

    if (count != 1)
        return luaL_error(L, "lerl_decoder.decodeFloat: Invalid float encoded.");

    lua_pushnumber(L, number);
    return 1;
}

static int lerl_decodeNewFloat(lua_State* L) {
    union {
        uint64_t ui64;
        double df;
    } val;

    val.ui64 = lerl_read64_out(L);

    lua_pushnumber(L, val.df);
    return 1;
}

static int lerl_decodeBig(lua_State* L, uint32_t digits) {
    uint8_t sign = lerl_read8_out(L);

    if (digits > 8)
        return luaL_error(L, "lerl_decoder.decodeBig: Unable to decode big ints larger than 8 bytes");

    uint64_t value = 0;
    uint64_t b = 1;
    for(uint32_t i = 0; i < digits; ++i) {
            uint64_t digit = lerl_read8_out(L);
            value += digit * b;
            b <<= 8;
    }

    if (digits <= 8) {
        if (sign == 0) {
            lua_pushinteger(L, value);
            return 1;
        } else if ((value & (1ULL << 63)) == 0) {
            lua_pushinteger(L, -(lua_Integer)value);
            return 1;
        }
    }

    char outBuffer[32] = {0};
    const char* fmt = (sign == 0) ? (PRIu64) : ("-" PRIu64);
    int res = sprintf(outBuffer, fmt, value);

    if (res < 0)
        return luaL_error(L, "lerl_decoder.decodeBig: Unable to convert big int to string.");

    lua_pushlstring(L, outBuffer, res);
    return 1;
}

static int lerl_decodeSmallBig(lua_State* L) {
    return lerl_decodeBig(L, lerl_read8_out(L));
}

static int lerl_decodeLargeBig(lua_State* L) {
    return lerl_decodeBig(L, lerl_read32_out(L));
}

static int lerl_decodeBinary(lua_State* L) {
    uint32_t size = lerl_read32_out(L);
    const char* data = lerl_readString(L, size);
    lua_pushlstring(L, data, size);
    return 1;
}

static int lerl_decodeString(lua_State* L) {
    uint16_t size = lerl_read16_out(L);
    lua_pushlstring(L, lerl_readString(L, size), size);
    return 1;
}

static int lerl_decodeStringAsList(lua_State* L) {
    lerl_decoder* the_decoder = lerl_get_decoder(L, 1);
    uint16_t length = lerl_read16_out(L);
    if (the_decoder->offset + length > the_decoder->size)
        return luaL_error(L, "lerl_decode.decodeStringAsList: Reading sequence past the end of the buffer.");

    lua_createtable(L, length, 0);

    for (uint16_t i = 1; i <= length; ++i) {
        lerl_decodeSmallInteger(L);
        lua_seti(L, -2, i);
    }
    return 1;
}

static int lerl_decodeSmallTuple(lua_State* L) {
    return lerl_decodeSequential(L, lerl_read8_out(L));
}

static int lerl_decodeLargeTuple(lua_State* L) {
    return lerl_decodeSequential(L, lerl_read32_out(L));
}

static int lerl_decodeCompressed(lua_State* L) {
    lerl_decoder* the_decoder = lerl_get_decoder(L, 1);
    uint32_t uncompressedSize = lerl_read32_out(L);

    unsigned long sourceSize = uncompressedSize;

    uint8_t* outBuffer = (uint8_t*)malloc(uncompressedSize);

    int ret = uncompress(outBuffer, &sourceSize, (const unsigned char*)(the_decoder->data + the_decoder->offset), (uLong)(the_decoder->size - the_decoder->offset));

    the_decoder->offset += sourceSize;

    if (ret != Z_OK) {
        free(outBuffer);
        return luaL_error(L, "lerl_decoder.decodeCompressed: Failed to uncompresss compressed item.");
    }

    lerl_decoder* children = lua_newuserdata(L, sizeof(lerl_decoder));
    children->data = outBuffer;
    children->size = uncompressedSize;
    children->invalid = false;
    children->empty_ref = the_decoder->empty_ref;
    children->offset = 0;

    luaL_getmetatable(L, lerl_decoder_type);
    lua_setmetatable(L, -2); // Stack : decoder, ... children
    lua_insert(L, 1); // Stack : children, decoder, ...
    lerl_unpack(L); // Stack : children, decoder, value, ...
    children->data = NULL;
    children->size = 0;
    children->offset = 0;
    free(outBuffer);

    lua_copy(L, 2, 1); // Stack: decoder, decoder, value, ...
    lua_remove(L, 2); // Stack: decoder, value, ...
    return 1;
}

static int lerl_decodeReference(lua_State* L) {
    lua_createtable(L, 0, 3);
    lua_pushliteral(L, "node");
    lerl_unpack(L);
    lua_settable(L, -3);

    lua_pushliteral(L, "ids");
    lua_createtable(L, 1, 0);
    lerl_read32(L);
    lua_rawseti(L, -2, 1);
    lua_settable(L, -3);

    lua_pushliteral(L, "creation");
    lerl_read8(L);
    lua_settable(L, -3);
    return 1;
}

static int lerl_decodeNewReference(lua_State* L) {
    uint16_t len = lerl_read16_out(L);

    lua_createtable(L, 0, 3);
    lua_pushliteral(L, "node");
    lerl_unpack(L);
    lua_settable(L, -3);

    lua_createtable(L, 0, 3);
    lua_pushliteral(L, "creation");
    lerl_read8(L);
    lua_settable(L, -3);

    lua_pushliteral(L, "ids");

    lua_createtable(L, len, 0);

    for (uint16_t i = 1; i <= len; ++i){
        lerl_read32(L);
        lua_rawseti(L, -2, i);
    }

    lua_settable(L, -3);
    return 1;
}

static int lerl_decodePort(lua_State* L) {
    lua_createtable(L, 0, 3);
    lua_pushliteral(L, "node");
    lerl_unpack(L);
    lua_settable(L, -3);

    lua_pushliteral(L, "id");
    lerl_read32(L);
    lua_settable(L, -3);

    lua_pushliteral(L, "creation");
    lerl_read8(L);
    lua_settable(L, -3);
    return 1;
}

static int lerl_decodePID(lua_State* L) {
    lua_createtable(L, 0, 4);
    lua_pushliteral(L, "node");
    lerl_unpack(L);
    lua_settable(L, -3);

    lua_pushliteral(L, "id");
    lerl_read32(L);
    lua_settable(L, -3);

    lua_pushliteral(L, "serial");
    lerl_read32(L);
    lua_settable(L, -3);

    lua_pushliteral(L, "creation");
    lerl_read8(L);
    lua_settable(L, -3);
    return 1;
}

static int lerl_decodeExport(lua_State* L) {
    lua_createtable(L, 0, 3);
    lua_pushliteral(L, "mod");
    lerl_unpack(L);
    lua_settable(L, -3);

    lua_pushliteral(L, "fun");
    lerl_unpack(L);
    lua_settable(L, -3);

    lua_pushliteral(L, "arity");
    lerl_unpack(L);
    lua_settable(L, -3);
    return 1;
}

static int lerl_unpack(lua_State* L) {
    lerl_decoder* the_decoder = lerl_get_decoder(L, 1);

    if (the_decoder->invalid)
        return luaL_error(L, "Unpacking an invalidated buffer");

    if (the_decoder->offset > the_decoder->size)
        return luaL_error(L, "Unpacking beyond the end of the buffer");

    uint8_t type = lerl_read8_out(L);

    switch(type) {
        case SMALL_INTEGER_EXT:
            lerl_decodeSmallInteger(L);
            return 1;
        case INTEGER_EXT:
            lerl_decodeInteger(L);
            return 1;
        case FLOAT_EXT:
            lerl_decodeFloat(L);
            return 1;
        case NEW_FLOAT_EXT:
            lerl_decodeNewFloat(L);
            return 1;
        case ATOM_EXT:
            lerl_decodeAtom(L);
            return 1;
        case SMALL_ATOM_EXT:
            lerl_decodeSmallAtom(L);
            return 1;
        case SMALL_TUPLE_EXT:
            lerl_decodeSmallTuple(L);
            return 1;
        case LARGE_TUPLE_EXT:
            lerl_decodeLargeTuple(L);
            return 1;
        case NIL_EXT:
            lerl_decodeNil(L);
            return 1;
        case STRING_EXT:
            lerl_decodeStringAsList(L);
            return 1;
        case LIST_EXT:
            lerl_decodeList(L);
            return 1;
        case MAP_EXT:
            lerl_decodeMap(L);
            return 1;
        case BINARY_EXT:
            lerl_decodeBinary(L);
            return 1;
        case SMALL_BIG_EXT:
            lerl_decodeSmallBig(L);
            return 1;
        case LARGE_BIG_EXT:
            lerl_decodeLargeBig(L);
            return 1;
        case REFERENCE_EXT:
            lerl_decodeReference(L);
            return 1;
        case NEW_REFERENCE_EXT:
            lerl_decodeNewReference(L);
            return 1;
        case PORT_EXT:
            lerl_decodePort(L);
            return 1;
        case PID_EXT:
            lerl_decodePID(L);
            return 1;
        case EXPORT_EXT:
            lerl_decodeExport(L);
            return 1;
        case COMPRESSED:
            lerl_decodeCompressed(L);
            return 1;
        default:
            return luaL_error(L, "Unsupported erlang term type identifier found");
    }
}

static int lerl_unpack_fun(lua_State* L) {
    if (lerl_unpack(L)) {
        return 1;
    } else {
        lerl_decoder* the_decoder = lerl_get_decoder(L, 1);
        lua_pushnil(L);
        lua_pushinteger(L, the_decoder->offset);
        return 2;
    }
}

static int lerl_unpack_all(lua_State* L) {
    lerl_decoder* the_decoder = lerl_get_decoder(L, 1);
    int count = 0;
    while((the_decoder->offset < the_decoder->size) && !the_decoder->invalid){
        count = count + 1;
        lerl_unpack(L);
    }
    lua_remove(L, 1);
    return count;
}

static int lerl_decoder_gc(lua_State* L) {
    lerl_decoder* the_decoder = lerl_get_decoder(L, 1);

    if (the_decoder->data != NULL)
        free(the_decoder->data);

    the_decoder->size = 0;
    the_decoder->invalid = true;
    the_decoder->offset = 0;
    lua_getfield(L, LUA_REGISTRYINDEX, "lerl_empty");
    int defaultref = lua_tointeger(L, -1);
    if (the_decoder->empty_ref != defaultref)
        luaL_unref(L, LUA_REGISTRYINDEX, defaultref);

    lua_pop(L, 1);
    return 0;
}

static int lerl_make_array(lua_State* L) {
    if (!lua_isnoneornil(L, 1)) {
        luaL_getmetatable(L, lerl_array_mt);
        lua_setmetatable(L, -2);
        return 1;
    } else {
        lua_newtable(L);
        luaL_getmetatable(L, lerl_array_mt);
        lua_setmetatable(L, -2);
        return 1;
    }
}


static int lerl_make_map(lua_State* L) {
    if (!lua_isnoneornil(L, 1)) {
        luaL_getmetatable(L, lerl_map_mt);
        lua_setmetatable(L, -2);
        return 1;
    } else {
        lua_newtable(L);
        luaL_getmetatable(L, lerl_map_mt);
        lua_setmetatable(L, -2);
        return 1;
    }
}

const luaL_Reg decoder_metamethods[] = {
    {"__gc", lerl_decoder_gc},
    {NULL, NULL}
};

const luaL_Reg decoder_methods[] = {
    {"unpack", lerl_unpack_fun},
    {"unpack_all", lerl_unpack_all},
    {"reset", lerl_reset_decoder},
    {"read8", lerl_read8},
    {"read16", lerl_read16},
    {"read32", lerl_read32},
    {"read64", lerl_read64},
    {NULL, NULL}
};

static int lerl_pack_encapsulated(lua_State* L) {
    lua_getfield(L, LUA_REGISTRYINDEX, "lerl_global_encoder");
    lua_insert(L, 1);
    lerl_pack_all(L);
    return lerl_release(L);
}

static int lerl_unpack_encapsulated(lua_State* L) {
    lua_getfield(L, LUA_REGISTRYINDEX, "lerl_global_decoder");
    lua_insert(L, 1);
    lua_settop(L, 2);
    lerl_reset_decoder(L);
    return lerl_unpack_all(L);
}

const luaL_Reg lerl_functions[] = {
    {"new_encoder", lerl_new_encoder},
    {"new_decoder", lerl_new_decoder},
    {"lerl_map", lerl_make_map},
    {"lerl_array", lerl_make_array},
    {"empty_decoder", lerl_empty_decoder},
    {"pack", lerl_pack_encapsulated},
    {"unpack", lerl_unpack_encapsulated},
    {NULL, NULL}
};

static int lerl_decoder_index(lua_State* L) {
    lerl_decoder* the_decoder = lerl_get_decoder(L, 1);
    size_t len;
    const char* key = luaL_checklstring(L, 2, &len);

    if (len == 6 && strncmp(key, "offset", 6) == 0) {
        lua_pushinteger(L, the_decoder->offset);
    } else if (len == 4 && strncmp(key, "size", 4) == 0) {
        lua_pushinteger(L, the_decoder->size);
    } else if (len == 7 && strncmp(key, "invalid", 7) == 0) {
        lua_pushboolean(L, the_decoder->invalid);
    } else {
        if (luaL_getmetafield(L, 1, "__index_table") == LUA_TTABLE) {
            lua_getfield(L, -1, key);
        } else {
            lua_pushnil(L);
        }
    }
    return 1;
}

static int lerl_decoder_init(lua_State* L) {
    luaL_newmetatable(L, lerl_decoder_type);
    luaL_setfuncs(L, decoder_metamethods, 0);
    lua_pushliteral(L, "__index_table");
    lua_createtable(L, 0, 2);
    luaL_setfuncs(L, decoder_methods, 0);
    lua_settable(L, -3);
    lua_pushliteral(L, "__index");
    lua_pushcfunction(L, lerl_decoder_index);
    lua_settable(L, -3);
    lua_pop(L, 1);
    return 1;
}

static const char *lerl_empty = "lerl_empty";

LUALIB_API int luaopen_lerl(lua_State* L) {
    luaL_newmetatable(L, lerl_array_mt);
    lua_pushliteral(L, "__lerl_type");
    lua_pushliteral(L, "array");
    lua_settable(L, -3);
    lua_pop(L, 1);

    luaL_newmetatable(L, lerl_map_mt);
    lua_pushliteral(L, "__lerl_type");
    lua_pushliteral(L, "map");
    lua_settable(L, -3);
    lua_pop(L, 1);

    lerl_encoder_init(L);
    lerl_decoder_init(L);

    lerl_new_encoder2(L, false);
    lua_setfield(L, LUA_REGISTRYINDEX, "lerl_global_encoder");

    lerl_empty_decoder(L);
    lua_setfield(L, LUA_REGISTRYINDEX, "lerl_global_decoder");

    lua_pushlightuserdata(L, (void*)lerl_empty);
    int default_empty = luaL_ref(L, LUA_REGISTRYINDEX);
    lua_pushinteger(L, default_empty);
    lua_setfield(L, LUA_REGISTRYINDEX, "lerl_empty");

    luaL_newlibtable(L, lerl_functions);
    luaL_setfuncs(L, lerl_functions, 0);

    lua_pushliteral(L, "empty");
    lua_pushinteger(L, default_empty);
    lua_settable(L, -3);
    return 1;
}

