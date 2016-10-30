module SetCommands
    def sadd(key, *members)
        send_cluster_command([:sadd, key, members])
    end

    def scard(key)
        send_cluster_command([:scard, key])
    end

    def sdiff(*keys)
        multiple_sets_command(:sdiff, keys)
    end

    def sdiffstore(dest, *keys)
        sadd(sdiff(keys))
    end

    def sinter(*keys)
        multiple_sets_command(:sinter, keys)
    end

    def sinterstore(dest, *keys)
        sadd(dest, sinter(keys))
    end

    def sismember(key, member)
        send_cluster_command([:sismember, key, member])
    end

    def smembers(key)
        send_cluster_command([:smembers, key])
    end

    def smove(source, destination, member)
        send_cluster_command([:smove, source, destination, member])
    end

    def spop(key, count = 1)
        send_cluster_command([:spop, key, count])
    end

    def srandmember(key, count = 1)
        send_cluster_command([:srandmember, key, count])
    end

    def srem(key, *members)
        send_cluster_command([:srem, key, members])
    end

    def sunion(*keys)
        multiple_sets_command(:sunion, keys)
    end

    def sunionstore(dest, *keys)
        sadd(dest, sunion(*keys))
    end

    def sscan(key, cursor, options = {})
        send_cluster_command([:sscan, key, cursor, options])
    end

    private

    def multiple_sets_command(command, keys)
        send_cluster_command([command] + keys)
    rescue Redis::CommandError => e
        if e.message.start_with?('CROSSSLOT')
            send("resolve_crossslot_#{command}".to_sym, keys)
        else
            raise e
        end
    end

    def resolve_crossslot_(keys, &set_join_func)
        keys.reduce({}) do |agg, key|
            values = smembers(key)
            if agg == {}
                agg = Set.new(values)
            else
                agg = set_join_func.call(agg, Set.new(values))
            end
            agg
        end.to_a
    end

    def resolve_crossslot_sunion(keys)
        resolve_crossslot_(keys) { |lhs, rhs| lhs |= rhs }
    end

    def resolve_crossslot_sinter(keys)
        resolve_crossslot_(keys) { |lhs, rhs| lhs &= rhs }
    end

    def resolve_crossslot_sdiff(keys)
        resolve_crossslot_(keys) { |lhs, rhs| lhs -= rhs }
    end
end
