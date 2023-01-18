package com.solace.maas.ep.event.management.agent.repository.scan;

import com.solace.maas.ep.event.management.agent.repository.model.scan.ScanTypeEntity;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ScanTypeRepository extends CrudRepository<ScanTypeEntity, String> {
    Optional<ScanTypeEntity> findByNameAndScanId(String name, String scanId);
}
