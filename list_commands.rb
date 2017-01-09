module ListCommands
    def blpop(*keys, timeout)
        assert_single_key_br(keys)
        send_cluster_command([:blpop, keys.first, timeout])
    end

    def brpop(*keys, timeout)
        assert_single_key_br(keys)
        send_cluster_command([:brpop, keys.first, timeout])
    end

    def brpoplpush(source, destination, timeout)
        value = send_cluster_command([:brpop, source, timeout])
        lpush(destination, value) unless value.nil?
    end

    def lindex(key, index)
        send_cluster_command([:lindex, key, index])
    end

    def linsert(key, where, pivot, value)
        send_cluster_command([:linsert, key, where, pivot, value])
    end

    def llen(key)
        send_cluster_command([:llen, key])
    end

    def llen(key)
        send_cluster_command([:lpop, key])
    end

    def lpush(key, *values)
        send_cluster_command([:lpush, key, values])
    end

    def lpushx(key, value)
        send_cluster_command([:lpushx, key, value])
    end

    def lrange(key, start, stop)
        send_cluster_command([:lrange, start, stop])
    end

    def lrem(key, count, value)
        send_cluster_command([:lrem, key, count, value])
    end

    def lset(key, index, value)
        send_cluster_command([:lset, key, index, value])
    end

    def ltrim(key, start, stop)
        send_cluster_command([:ltrim, key, start, stop])
    end

    def rpop(key)
        send_cluster_command([:rpop, key])
    end

    def rpoplpush(source, destination)
        value = send_cluster_command([:rpop, source])
        lpush(destination, value) unless value.nil?
    end

    def rpush(key, *values)
        send_cluster_command([:rpush, key, values])
    end

    def rpushx(key, value)
        send_cluster_command([:rpushx, key, value])
    end

    private

    def assert_single_key_br(keys)
        # HACK: since the vast majority of blocking pops in Redis are on a single
        # key to remove polling, support only for s single key blocking pop is
        # deemed acceptable for Redis Cluster use. Otherwise, threading would be
        # required to support blocking pops on keys since the keys are likely to
        # be on multiple nodes.
        if keys.length > 1
            raise 'cluster blocking list pop commands currently only supports a single key'
        end
    end
end
