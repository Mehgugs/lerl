local lerl = require"lerl"

local helloWorldList = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
local helloWorldBinary = '\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B'

local helloWorldListWithNull = {1, 2, 3, 4, 5, 0, 6, 7, 8, 9, 10, 11}
local helloWorldBinaryWithNull = '\x01\x02\x03\x04\x05\x00\x06\x07\x08\x09\x0A\x0B'


describe("unpacks", function()
    it('short list via string with null byte', function()
        local D = lerl.new_decoder('\x83k\x00\x0c' .. helloWorldBinaryWithNull)
        assert.are_same(
            D:unpack(),
            helloWorldListWithNull
        )
    end)
    it('short list via string without byte', function()
        local D = lerl.new_decoder('\x83k\x00\x0b' .. helloWorldBinary)
        assert.are_same(
            D:unpack(),
            helloWorldList
        )
    end)
    it('binary with null byte', function()
        local D = lerl.new_decoder('\x83m\x00\x00\x00\x0chello\x00 world')
        assert.are_same(
            D:unpack(),
            'hello\x00 world'
        )
    end)
    it('binary without null byte', function()
        local D = lerl.new_decoder('\x83m\x00\x00\x00\x0bhello world')
        assert.are_same(
            D:unpack(),
            'hello world'
        )
    end)
    it('map', function()
        local D = lerl.new_decoder('\x83t\x00\x00\x00\x03a\x02a\x02a\x03l\x00\x00\x00\x03a\x01a\x02a\x03jm\x00\x00\x00\x01aa\x01')
        assert.are_same(
            D:unpack(),
            {
                a = 1,
                [2] = 2,
                [3] = {1,2,3}
            }
        )
    end)
    it('booleans', function()
        local D = lerl.new_decoder('\x83s\x05falses\x04true')
        local f,t = D:unpack_all()
        assert(t == true and f == false)
    end)

    it('nil token', function()
        local D = lerl.new_decoder('\x83j')
        assert.are_same(D:unpack(), {})
    end)

    it('nil atom', function()
        local D = lerl.new_decoder('\x83s\x03nil')
        assert.are_same(D:unpack(), nil)
    end)

    it('floats', function()
        local D = lerl.new_decoder'\x83c2.50000000000000000000e+00\x00\x00\x00\x00\x00'
        assert.are_same(D:unpack(), 2.5)
    end)

    it('new floats', function()
        local D = lerl.new_decoder'\x83F\x40\x04\x00\x00\x00\x00\x00\x00'
        assert.are_same(D:unpack(), 2.5)
    end)

    it('small ints', function()
        local D = lerl.empty_decoder()
        local fmt = (">BBB")
        for i = 0, 255 do
            D:reset(fmt:pack(0x83, 97, i))
            assert.are_equal(D:unpack(), i)
        end
    end)
end)