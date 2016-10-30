module StringCommands
    def append(key, value)
        send_cluster_command([:append, key, value])
    end

    def bitcount(key, start = 0, stop = -1)
        send_cluster_command([:bitcount, key, start, stop])
    end

    # bitfield, passthrough via method_missing

    def bitop(operation, destkey, *keys)
        if operation == "NOT" and keys.length > 1
            raise(ArgumentError, 'bitop "NOT" is only valid for a single-key')
        end

        v = nil
        op = case operation
	         when "AND" then lambda { |l, r| l & r }
                 when  "OR" then lambda { |l, r| l | r }
                 when "XOR" then lambda { |l, r| l ^ r }
                 when "NOT" then lambda { |r| bitwise_not(r) }
             end
        keys.each do |key|
            if v.nil?
                v = get_bit_value(key)
            else
                v =  op.call(v, get_bit_value(key))
            end
        end
        v = op.call(v) if operation == "NOT"
        set_bit_value(destkey, v)
        0
    end

    def bitpos(key, bit, start = nil, stop = nil)
        send_cluster_command([:bitpos, key, bit, start, stop])
    end

    def decr(key)
        send_cluster_command([:decr, key])
    end

    def decrby(key, decrement)
        send_cluster_command([:decrby, key, decrement])
    end

    def del(*keys)
        keys.each {|key| send_cluster_command([:del, key])}
        nil
    end

    def exists(key)
        send_cluster_command([:exists, key])
    end

    def get(key)
        send_cluster_command([:get, key])
    end

    def getbit(key, offset)
        send_cluster_command([:getbit, key, offset])
    end

    def getrange(key, start, stop)
       send_cluster_command([:getrange, key, start, stop])
    end

    def getset(key, value)
        send_cluster_command([:getset, key, value])
    end

    def incr(key)
        send_cluster_command([:incr, key])
    end

    def incrby(key, increment) 
        send_cluster_command([:incrby, key, increment])
    end

    def incrbyfloat(key, increment)
        send_cluster_command([:incrbyfloat, key, increment])
    end

    def mget(*keys)
        keys.map { |key| get(key) }
    end

    def mset(*args)
        each_over_args(args) { |k, v| set(k, v) }
    end

    def msetnx(*args)
        each_over_args(args, true) { |k, v| exists(k) }
        each_over_args(args) { |k, v| setnx(k, v) }
    end

    def psetex(key, milliseconds, value)
        send_cluster_command([:psetex, key, milliseconds, value])
    end

    def set(key, value, options = {})
        send_cluster_command([:set, key, value, options])
    end

    def setbit(key, offset, value)
        send_cluster_command([:setbit, key, offset, value])
    end

    def setex(key, seconds, value)
        send_cluster_command([:setex, key, seconds, value])
    end

    def setnx(key, value)
        send_cluster_command([:setnx, key, value])
    end

    def setrange(key, offset, value)
        send_cluster_command([:setrange, key, offset, value])
    end

    def strlen(key)
        send_cluster_command([:strlen, key])
    end

    private

    def get_bit_value(key)
        send_cluster_command([:bitfield, key, 'GET', 'u8', 0])[0]
    end

    def bitwise_not(value)
        value.to_s(2).tr("0", "s").tr("1", "0").tr("s", "1").to_i(2)
    end

    def set_bit_value(key, value)
        bits = value.to_s(2)
        i = 0
        bits.each_byte do |byt|
            it = byt - 48
            setbit(key, i, it)
            i += 1
        end
    end

    def each_over_args(args, short_on_call = false, &block)
        success = false
        k = nil
        args.each do |a|
            if k.nil?
                k = a
            else
                success = block.call(k, a)
                if short_on_call
                  puts "short on call, success: #{success} for key: #{key}"
                  if  !success
                    return false
                  end
                end
                k = nil
            end
        end
        success
    end
end
