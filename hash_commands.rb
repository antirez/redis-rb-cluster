module HashCommands
    def hdel(key, *fields)
        send_cluster_command([:hdel, key, fields])
    end

    def hexists(key, field)
        send_cluster_command([:hexists, key, field])
    end

    def hget(key, field)
        send_cluster_command([:hget, key, field])
    end

    def hgetall(key)
        send_cluster_command([:hgetall, key])
    end

    def hincrby(key, field, increment)
        send_cluster_command([:hincrby, key, field, increment])
    end

    def hincrbyfloat(key, field, increment)
        send_cluster_command([:hincrbyfloat, key, field, increment])
    end

    def hkeys(key)
        send_cluster_command([:hkeys, key])
    end

    def hlen(key)
        send_cluster_command([:hlen, key])
    end

    def hmget(key, *fields)
        send_cluster_command([:hmget, key, fields])
    end

    def hmset(key, *field_values)
        send_cluster_command([:hmset, key, field_values])
    end

    def hset(key, field, value)
        send_cluster_command([:hset, key, field, value])
    end
    
    def hsetnx(key, field, value)
        send_cluster_command([:hsetnx, key, field, value])
    end

    def hstrlen(key, field)
        send_cluster_command([:hstrlen, key, field])
    end

    def hvals(key)
        send_cluster_command([:hvals, key])
    end

    def hscan(key, cursor, options = {})
        send_cluster_command([:hscan, key, cursor, options])
    end
end
