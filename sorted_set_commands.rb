module SortedSetCommands
    def zadd(key, *args)
        send_cluster_command([:zadd, key, args])
    end

    def zcard(key)
        send_cluster_command([:zcard, key])
    end

    def zcount(key, min, max)
        send_cluster_command([:zcount, key, min, max])
    end

    def zincrby(key, increment, member)
        send_cluster_command([:zincrby, key, increment, member])
    end

    def zinterstore(dest, keys)
        s = nil
        keys.each do |key|
            sk = Set.new()
            cursor = -1
            until cursor == "0"
                scanresult = zscan(key, cursor)
                cursor = scanresult[0]
                scanresult[1].each {|member, score| sk.add([member, score]) }
            end
            if s.nil?
                s = sk
            else
                s &= sk
            end
            break if s.length <= 0
        end
        res = multi do
            del(dest)
            if s.length > 0
                zadd(dest, *s.to_a)
            end
        end
        res[-1]
    end

    def zlexcount(key, min, max)
        send_cluster_command([:zlexcount, key, min, max])
    end

    def zrange(key, start, stop, options = {})
        send_cluster_command([:zrange, key, start, stop, options])
    end

    def zrangebylex(key, min, max, options = {})
        send_cluster_command([:zrangebylex, key, min, max, options])
    end

    def zrevrangebylex(key, max, min, options = {})
        send_cluster_command([:zrevrangebylex, key, max, min, options])
    end

    def zrangebyscore(key, min, max, options = {})
        send_cluster_command([:zrangebyscore, key, min, max, options])
    end

    def zrank(key, member)
        send_cluster_command([:zrank, key, member])
    end

    def zrem(key, *members)
        send_cluster_command([:zrem, key, members])
    end

    def zremrangebylex(key, min, max)
        send_cluster_command([:zremrangebylex, key, min, max])
    end

    def zremrangebyrank(key, start, stop)
        send_cluster_command([:zremrangebyrank, key, start, stop])
    end

    def zremrangebyscore(key, min, max)
        send_cluster_command([:zremrangebyscore, key, min, max])
    end

    def zrevrank(key, member)
        send_cluster_command([:zrevrank, key, member])
    end

    def zscore(key, member)
        send_cluster_command([:zscore, key, member])
    end

    def zunionstore(dest, keys)
        h = {}
        keys.each do |key|
            cursor = -1
            until cursor == "0"
                scanresult = zscan(key, cursor)
                cursor = scanresult[0]
                scanresult[1].each {|member, score| h[member] = (h[member] || 0) + score }
            end
            member_scores = h.collect {|member, score| [score, member] }.flatten()
            res = multi do
                del(dest)
                zadd(dest, member_scores)
            end
            res[-1]
        end
    end

    def zscan(key, cursor, options = {})
        send_cluster_command([:zscan, key, cursor, options])
    end
end
