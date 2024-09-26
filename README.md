# fairq

a postgres-based fair task queue for multi-tenant thingies

> [!WARNING]  
> Do not use this yet under any circumstance, still a wip and probably broken

### todo

- [x] add tasks in bulk
- [x] pop jobs in bulk
- [x] minimal worker
- [x] lock task addition by lookup key
- [x] callback
- [ ] drop tasks / update as 'failed' on callback result
- [ ] fault tolerance
- [ ] queue monitoring
- [ ] standalone binary server
- [ ] roll index allocation when over capacity
